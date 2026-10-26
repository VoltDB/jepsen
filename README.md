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
starts with `VOLTDB_OPTS=-javaagent:...=listener:true,port:9091`. The listener
binds to localhost on the node; `jepsen.voltdb.byteman` loads and unloads rules
by running Byteman's submit client on the node over SSH. Nothing needs to be
installed on the node images beyond the JDK.

```
lein run test --tarball file://voltdb-ent-<version>.tar.gz -w single --nemesis partition --byteman
```

With no rules loaded the agent only adds its listener thread. See
`resources/byteman/README.md` for how rules are written.
