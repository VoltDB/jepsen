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
