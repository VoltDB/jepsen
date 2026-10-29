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

## Byteman fault injection

`--byteman` attaches the [Byteman](https://byteman.jboss.org/) agent to every
VoltDB server JVM (ENG-30396). The agent and submit jars are dependencies in
`project.clj`; setup uploads them, plus every rule file in
`resources/byteman/`, to `<base-dir>/byteman/` on each node, and the server
starts with `VOLTDB_OPTS=-javaagent:...=listener:true,port:9091,boot:...`. The listener
binds to localhost on the node; `jepsen.voltdb.byteman` loads and unloads rules
by running Byteman's submit client on the node over SSH. Nothing needs to be
installed on the node images beyond the JDK.

```
lein run test --tarball file://voltdb-ent-<version>.tar.gz -w single --nemesis partition --byteman
```

With no rules loaded the agent only adds its listener thread.
[resources/byteman/README.md](resources/byteman/README.md) is the guide to
writing a rule and adding a fault, for example to force a window you suspect
while chasing a bug. `resources/byteman/check-rules.sh` type-checks rules
against a VoltDB build.

### Byteman faults

Each Byteman fault is a nemesis fault name; naming one turns on `--byteman`
itself. `--nemesis byteman-rules` runs the plan's first rule set (every fault
below except `replay-error`). Faults run one at a time in round-robin order,
every `--nemesis-interval` seconds on average. Several restart nodes or the
whole cluster, so allow about two minutes of `--time-limit` per fault.

```
lein run test --tarball file://voltdb-ent-<version>.tar.gz -w single --nemesis byteman-rules --time-limit 600
```

| Fault | Rule file | What a run does | Passes when |
|---|---|---|---|
| `snapshot-io-error` | `snapshot-write-ioexception.btm` | Loads the rule on every node that is up, takes a blocking `@SnapshotSave` with a `BYTEMAN_<millis>` nonce, waits for `@Statistics SNAPSHOTSTATUS` to settle, unloads | SNAPSHOTSTATUS reports FAILURE for the nonce |
| `client-response-delay` | `client-response-delay.btm` | Loads the rule on every node for 10 s: every 25th client response waits 1.5 s on its network thread, past the client's 1 s timeout | The workload checker passes |
| `promotion-delay` | `promotion-delay.btm` | Loads the rule on all nodes but one, kills that one; the survivors' new leaders wait 3 s before repairing; rejoins the killed node | Leaders were promoted, the node rejoins, the workload checker passes |
| `rejoin-kill` | `rejoin-kill.btm` | Kills a node and rejoins it with the rule in its startup script: 2 s into the stream snapshot the rule kills it again; rejoins it once more without the rule | The rule killed the rejoining node, the second rejoin succeeds |
| `replay-delay` | `replay-delay.btm` | Restarts the whole cluster with the rule in every node's startup script. Each partition's log is replayed by one host, so only the replaying hosts (usually one, recorded as `:replayed-on`) wait 100 ms on each of their first 50 transactions | Recovery completes, the workload checker passes |
| `replay-error` | `replay-error.btm` | As `replay-delay`, but each replaying host's 5th transaction throws; then restarts the cluster without the rule | The replaying hosts crash (with a FATAL, which the Jenkins job counts as a failure, so it is not in `byteman-rules`) and the clean recovery completes |

For every run the nemesis also counts the rule's `fired` lines on each node
and checks that every node up before the run is up after it. The `byteman`
checker fails the test on any run with problems (`:never-fired`,
`:nodes-down-after` and the fault-specific ones above), and on any enabled
fault that never ran. Each run's op value in history.edn holds the details:
`:fired` per node, `:down-after`, `:problems`, the victim node and the
fault's own results.

Rules that must be in place when a server JVM starts, before one could be
submitted (rejoin and replay), go in `<base-dir>/byteman/boot.btm`, which
the agent loads at startup (`script:` option). It is empty except while such
a fault runs.

## LazyFS

`--lazyfs` puts each node's voltdbroot, and so its command log, snapshots,
config and catalog, on [LazyFS](https://github.com/dsrhaslab/lazyfs) 0.3.1
(commit `045a0b3`, plus our `resources/lazyfs/statfs.patch`), a FUSE
filesystem that holds written data in its own cache until it is fsynced.
Dropping that cache after killing VoltDB loses what a power loss would. On
its own, `--lazyfs` changes no fault: a killed node keeps its unsynced writes
as before. `--nemesis power-loss` (below) is the fault that drops them.

```
lein run test -w single --nemesis kill --lazyfs ...
lein run test -w set --nemesis power-loss --node-count 3 --kfactor 1 ...
```

What it does on each node, in `src/jepsen/voltdb/lazyfs.clj`:

- **Installs.** Installs `g++ cmake libfuse3-dev fuse3 git` with apt, then
  builds LazyFS with our patches applied in
  `/opt/jepsen-lazyfs/<hostname>/<commit>+statfs`, once per node (about 2
  minutes); later runs reuse it. It creates `/dev/fuse` if missing.
  It needs root, apt and GitHub access on the nodes.
- **Mounts.** Mounts LazyFS at `/tmp/jepsen-voltdb/voltdbroot`, backed by
  `/tmp/jepsen-voltdb/voltdbroot.lazyfs/data`, before `voltdb init` fills it.
- **Moves VoltDB's log off the mount.** `volt.log` would otherwise be in
  `voltdbroot/log`, where a power loss drops whatever log4j had not fsynced.
  `bin/voltdb` runs with `LOG4J_VOLTSERVER_PATH` set to a copy of the kit's
  `log4j2/voltserver.properties` whose log directory is
  `/tmp/jepsen-voltdb/log`.
- **Logs.** Saves `voltdbroot.lazyfs/log` with the other logs. It ends with
  LazyFS's cache usage and the files that held unsynced data at the end of
  the test.

### Things worth knowing

- **The statfs patch.** Upstream LazyFS does not implement `statfs`, so its
  mount reports 0 bytes free, and VoltDB refuses to start with under 1 MB
  free in voltdbroot (`FsUtils.ensureMinimalFreeSpace`). The patch adds an
  `lfs_statfs` that reports the backing directory's filesystem, as other
  passthrough FUSE filesystems do. It changes no read or write path. Before
  it, only the command log and snapshot directories could go on LazyFS, which
  left out files every restart rewrites (see the power loss results).
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

### Power loss

`--nemesis power-loss` (turns on `--lazyfs`) cuts power to every node at once,
about every `--nemesis-interval` seconds:

1. SIGKILLs VoltDB on every node and waits until it has exited, so none of
   its writes can reach LazyFS after the next step.
2. Drops every write LazyFS holds that was not fsynced, after logging which
   files held unsynced data (in `lazyfs.lazyfs/log`).
3. Starts every node, which recovers the cluster from the command log, and
   waits until all of them answer clients, like `cluster-restart`.

With a synchronous command log, VoltDB fsyncs a transaction before it
acknowledges it, so no acknowledged write may be lost; the workload checkers
verify that. The op's value records `:recovered-in-ms`, or `:still-down`
nodes, which fail the test.

**Only the command log and snapshots lose writes, by default (ENG-30442).**
VoltDB does not fsync the files `voltdb init`, catalog updates and recovery
write under voltdbroot (`.initialized`, `license.xml`, `config/`, export
cursors). A kernel writes them back within about 30 s; LazyFS never does.
Losing them leaves a node that fails with "The database root directory has
changed" (after init), or an empty `catalog.jar` and `deployment.yaml` that
VoltDB accepts as a default deployment, after which command log replay hangs
with no error (after DDL). Every recovery and the workload's DDL rewrite those
files seconds before the next power loss, so before step 2 every node fsyncs
every file outside `command_log/`, `command_log_snapshot/` and `snapshots/`,
standing in for that writeback. The test then checks what VoltDB does fsync.
`--lose-unsynced-config` skips that fsync, to check a fix for ENG-30442.

`--node-count N` runs on the first N of the nodes given with `-n`, e.g. a
3-node cluster from the lab's 5-node pool. It is applied before
`--concurrency 4n` is resolved.

#### Results

jenkins-st `matrix-systemtest-apprunner-jepsen` / `ENG-30396-lazyfs`, builds
#4 and #5: 3 nodes, k=1, synchronous command log, 300 s per workload,
`--nemesis-interval 30`.

| Workload | Power losses | Recovery (s) | Result |
|---|---|---|---|
| set | 8 | 16.7 | PASS |
| rw-register | 12 | 16.7 | PASS |
| list-append | 8 | 16.7-21.7 | PASS |
| bank | 10 | 16.7 | PASS |
| redundant-register | 9 | 16.7-21.7 | PASS |
| single | 9 | 16.7 | PASS |

No acknowledged write was lost, and every recovery brought all 3 nodes back.

- **What was dropped.** At almost every power loss, the only unsynced data
  was the 512 KB command log tracker (`command_log/tracker/commandlog-*`),
  which only `voltdb recover --safemode` reads. Once, 4 KB of a command log
  segment (`command_log/segments/loaned.1`) on one node was dropped. That
  cell (bank) still passed.
- **What recovery wrote outside LazyFS.** These runs had only the command
  log and snapshot directories on LazyFS. Every restart rewrote
  `config/cluster.properties`, `config/config.json`,
  `config/deployment.yaml`, `config/path.properties`, `config/catalog.jar`
  and `export_cursor/export_cursor_{0,1}` under voltdbroot. A power loss
  during or just after a restart could tear these, and the next recovery
  reads them. The statfs patch puts them on LazyFS too.

With all of voltdbroot on LazyFS (statfs patch) and the fsync of everything
outside the command log and snapshots at each power loss, build #9, same
settings:

| Workload | Power losses | Recovery (s) | Result |
|---|---|---|---|
| single (k=0) | 10 | 16.6 | PASS |
| set | 13 | 16.6 | PASS |
| rw-register | 9 | 16.6-26.6 | PASS |
| list-append | 8 | 16.6-21.6 | PASS |
| bank | 8 | 16.6 | PASS |
| redundant-register | 9 | 16.6-21.6 | PASS |
| single | 12 | 16.6 | PASS |

No acknowledged write was lost, and every recovery brought all 3 nodes back.
As before, the command log tracker was dropped at every power loss on two or
three nodes, and part of `command_log/segments/loaned.1` on one node twice
(list-append, single). Without the fsync, builds #6 and #8 never recovered
from the first power loss (ENG-30442, ENG-30444, ENG-30445).
