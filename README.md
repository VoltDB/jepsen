# jepsen.voltdb

Jepsen tests for voltdb.

## Quickstart

You'll need a VoltDB tarball to install, either as a local file or an HTTP URL.
You'll also need a license file in this directory, by default called `license.xml`.

The control node (where `lein` runs) needs JDK 21 or newer: this project
depends on jepsen 0.3.14, and jepsen 0.3.10+ requires JDK 21. The DB nodes can
stay on the JDK the server kit is qualified for (17). Set `VOLT_JAVA_HOME` to
the JDK path to use on the DB nodes for `voltdb init`, `voltdb start` and
`sqlcmd`; it defaults to the control node's `JAVA_HOME`.

```
export JAVA_HOME=/opt/oracle_java21
export VOLT_JAVA_HOME=/opt/oracle_java17
```

To run a single-key linearizability test with network partitions:

```
lein run test --tarball file://voltdb-ent-12.3.1.tar.gz -w single --nemesis partition
```

To run a full suite of tests with different workloads and nemeses:

```
lein run test-all --tarball file://voltdb-ent-12.3.1.tar.gz
```

To re-analyze a specific test with the current code:

```
lein run analyze store/whatever/some-timestamp/test.jepsen
```

To run a web server for browsing results in store/:

```
lein run serve
```

To build a fat jar that does all of the above:

```
lein uberjar
```

## Usage

```
lein run test --help
```

## Running

You may need to disable transparent huge pages on DB nodes (or, if running on LXC, on the host OS):

```
sudo bash -c "echo never > /sys/kernel/mm/transparent_hugepage/enabled"
sudo bash -c "echo never > /sys/kernel/mm/transparent_hugepage/defrag"
```

## LazyFS

`--lazyfs` puts each node's command log, command log snapshots and snapshots on
[LazyFS](https://github.com/dsrhaslab/lazyfs) 0.3.1 (commit `045a0b3`), a FUSE
filesystem that holds written data in its own cache until it is fsynced.
Dropping that cache after killing VoltDB loses what a power loss would. This
milestone only mounts it: the workloads and nemeses are unchanged, and a killed
node keeps its unsynced writes as before.

```
lein run test -w single --nemesis kill --lazyfs ...
```

What it does on each node, in `src/jepsen/voltdb/lazyfs.clj`:

- **Installs.** Installs `g++ cmake libfuse3-dev fuse3 git` with apt, then
  builds LazyFS in `/opt/jepsen-lazyfs/<hostname>/<commit>`, once per node
  (about 2 minutes); later runs reuse it. It creates `/dev/fuse` if missing.
  It needs root, apt and GitHub access on the nodes.
- **Mounts.** Mounts LazyFS at `/tmp/jepsen-voltdb/lazyfs`, backed by
  `/tmp/jepsen-voltdb/lazyfs.lazyfs/data`. The deployment's `<paths>` put
  `command_log`, `command_log_snapshot` and `snapshots` there, and
  `--file-corruption-dirs` follows them.
- **Logs.** Saves `lazyfs.lazyfs/log` with the other logs. It ends with
  LazyFS's cache usage and the files that held unsynced data at the end of
  the test.

### Things worth knowing

- **voltdbroot is not on LazyFS.** LazyFS has no `statfs`, so it reports 0
  bytes free, and VoltDB refuses to start with under 1 MB free in voltdbroot
  (`FsUtils.ensureMinimalFreeSpace`). voltdbroot's `config/` and markers
  are written once by `voltdb init`, before any fault, so recovery depends on
  nothing written there during the test.
- **Eviction is on.** We use our own LazyFS config rather than Jepsen's. With
  eviction off, LazyFS never frees a page (an fsync only marks it clean), and
  once the 0.5 GB cache is full it writes straight through, so nothing written
  after that could be lost. With eviction on, it writes dirty pages back
  before reusing them, as a kernel would.
- **Only file contents can be lost.** LazyFS caches data, not metadata:
  creates, renames and deletes reach the backing directory at once, and it has
  no `fsyncdir`. A test on LazyFS cannot lose a new file, such as a snapshot's
  `.finished` marker, or undo a rename.
- **No `fallocate`.** VoltDB's command log preallocation (`posix_fallocate`)
  falls back to glibc's emulation, which writes to every block, so the
  segments go through LazyFS's cache like any other write. No preallocation
  warnings appeared in the lab runs.
- **Jepsen's own `jepsen.lazyfs`.** We use its mount bookkeeping, fifo and
  unmount, but not its install. That install needs Debian 13's `libfuse3-4`
  package, which the Ubuntu 22.04 lab nodes lack. It also builds and mounts
  from `/opt/jepsen/lazyfs`, a single directory that the lab nodes share.

### Results

jenkins-st `matrix-systemtest-apprunner-jepsen`, branch
`ENG-30396-test-harness-expansion`, `CASE=single` (14 cells, 5 nodes, k=4
unless noted), `JEPSEN_EXTRA_ARGS=--lazyfs`:

| Build | LazyFS config | Result |
|---|---|---|
| #7 | eviction off (Jepsen's default) | 14/14 PASS; command log and replay on LazyFS on every node |
| #8 | eviction on, cache report | 14/14 PASS |
| #9 | no `--lazyfs` (baseline) | 14/14 PASS |
| #10 | eviction on, cache report (rerun of #8, whose artifacts were discarded) | 14/14 PASS |

In #10 all 70 node logs (14 cells, 5 nodes) end with the cache report. Cache
usage was 75.0-75.2% on every node: the three preallocated 128 MB command log
segments fill 384 MB of the 512 MB cache, so these 200-second runs never
needed to evict. `file-corruption` truncated files under
`/tmp/jepsen-voltdb/lazyfs/command_log` and `.../command_log_snapshot`.

**Overhead.** Throughput is no use for this: it is set by when the nemesis
strikes and varies several-fold between runs of the same cell with or without
LazyFS. Median latency of acknowledged operations (ms) in the cells whose load
is steady:

| Cell | #9 baseline | #7 LazyFS | #10 LazyFS |
|---|---|---|---|
| set:packet | 2.8 | 3.9 | 4.1 |
| set:membership | 2.7 | 3.6 | 4.2 |
| set:cluster-restart | 2.9 | 4.3 | 4.9 |
| set:file-corruption,cluster-restart | 3.0 | 4.3 | 5.7 |
| single:partition | 2.3 | 3.2 | 3.3 |
| redundant-register:kill | 3.6 | 4.8 | 5.2 |
| dr:kill:1 | 2.9 | 3.8 | 5.0 |

LazyFS adds about 1-2 ms to each acknowledged write, which fits every
synchronous command log fsync now going through FUSE. set:packet is
rate-limited and did the same 98 ok/s in #9 and #10, so the overhead shows up
as latency, not lost throughput.

**Unsynced at the end of a test.** In 22 of the 70 node logs, the only file
holding unsynced data was the 512 KB command log tracker
(`command_log/tracker/commandlog-*`). VoltDB memory-maps it and never forces
it; `voltdb recover --safemode` reads it to stop replay at the last finished
transaction. A power loss can leave it stale, which matters only for safe-mode
recovery. Nothing else was ever left unsynced.

## License

Copyright © 2016 Jepsen, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.

## jepsen maven repo
https://mvnrepository.com/artifact/jepsen/jepsen?repo=clojars

## jepsen github
https://github.com/jepsen-io/voltdb

## clojure repo
https://clojure.org/releases/downloads_older
