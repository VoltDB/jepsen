# Byteman rules

[Byteman](https://byteman.jboss.org/) rules inject delays, exceptions and
crashes at exact points in the VoltDB server's Java code while a Jepsen test
runs (ENG-30396). Use them to hit a narrow window on purpose, such as a
rejoin, a leader promotion, or a snapshot or command log write, instead of
waiting for a network fault to land there by chance. The test's checkers
then judge whether the cluster handled it.

This directory holds the rule files (`*.btm`), one per fault, and
`check-rules.sh`. Setup uploads every `*.btm` to `<base-dir>/byteman/rules/`
on each node, next to the agent jar. The faults that load them live in
`src/jepsen/voltdb/byteman_faults.clj`.

- [Rules](#rules)
- [Adding a rule](#adding-a-rule)
- [Rule cookbook](#rule-cookbook)
- [Byteman reference](#byteman-reference)
- [Pitfalls](#pitfalls)

## Rules

| File | Window | Fault | Nemesis |
|---|---|---|---|
| `snapshot-write-ioexception.btm` | `DefaultSnapshotDataTarget` write task, table-data block write | `IOException` from `FileChannel.write`, only for snapshots whose nonce starts with `BYTEMAN_` | `snapshot-io-error` |
| `client-response-delay.btm` | `ClientInterface$ClientResponseWork.getSerializedSize`, after commit | Every 25th response held 1.5 s | `client-response-delay` |
| `promotion-delay.btm` | `SpPromoteAlgo` / `MpPromoteAlgo.repairSurvivors` | 3 s delay before repair, under the deliver lock | `promotion-delay` |
| `rejoin-kill.btm` | `StreamSnapshotSink.processMessage` on the rejoining node | 2 s delay, then `killJVM()` | `rejoin-kill` |
| `replay-delay.btm` | `CommandLogReinitiatorImpl$TaskMessageConsumer.run` on the replaying hosts | 100 ms on each of the first 50 replayed transactions | `replay-delay` |
| `replay-error.btm` | Same, with the throw in `Iv2TransactionCreator.createTransaction` | `RuntimeException` on the 5th replayed transaction | `replay-error` |

`--nemesis byteman-rules` runs all of these except `replay-error`, whose
expected FATAL fails the Jenkins job. See the main README for what each
fault does and when it passes.

## Adding a rule

The usual case: you are chasing a bug, you suspect a window (a race, an
error path that is never exercised), and you want a Jepsen run that forces
it.

### 1. Name the window and the correct behaviour

Write down, before any code, where the fault goes and what the cluster must
do about it: "if the snapshot write fails, the snapshot is reported failed
and the cluster keeps serving". That sentence becomes the rule file's header
and decides what the fault's run checks. The checkers Jepsen already runs
(the workload checker, recovery, unhandled exceptions) cover data
correctness. The fault adds whatever is specific to the window.

### 2. Find the trigger point

Read the code on the branch you are testing. Byteman matches classes and
methods by name, so get the exact ones:

- **Nested and anonymous classes** are `Outer$Inner` and `Outer$1`,
  `Outer$2`, numbered in source order. List them, and see which one holds
  the call you want, from the server jar:

  ```
  unzip -l voltdb/voltdb-*.jar | grep 'org/voltdb/DefaultSnapshotDataTarget'
  javap -cp voltdb/voltdb-<version>.jar -c 'org.voltdb.DefaultSnapshotDataTarget$3' | grep -n 'invoke'
  ```

- **Lambdas** compile to methods named `lambda$<method>$<n>`; target the
  method they call instead where you can.
- **Which node.** Decide where the rule must be: every node, the survivors
  of a kill, only a rejoining node, or only the hosts doing some work. For
  example, command log replay runs on one host per partition, and rejoin
  streams to the joiner.
- **When the JVM needs it.** A rule for work that starts as the server
  starts, such as rejoin or replay, must be in the JVM before startup. See
  [the startup recipe](#rule-in-place-at-startup).

### 3. Write the rule file

One file per fault, named after it. Start from this:

```
# Window: <class and method, and what happens there>.
#
# Fault: <what the rule does, how often, on which nodes>.
#
# Correct behaviour: <what the cluster must do; what the checkers see>.

RULE <label>
CLASS org.voltdb.<Class>
METHOD <method>
AT ENTRY
IF <condition that limits the fault to what you mean>
DO traceln("BYTEMAN <label> fired");
   <action>
ENDRULE
```

Every rule must write `BYTEMAN <label> fired` with `traceln` when it fires.
The line goes to the server's stdout.log, and the nemesis counts these
lines to prove the fault happened. A run where the rule never fired fails
the test, because a renamed method would otherwise turn the fault into a
silent no-op. For a file with several rules, give them the same `<label>`
in the traceln and distinct rule names (`<label> tag`, `<label> throw`).

Keep the condition narrow. A rule stays loaded until it is unloaded and
fires on every matching call in the JVM, including VoltDB's own background
work (command log truncation snapshots, for example).

### 4. Type-check it

```
resources/byteman/check-rules.sh <voltdb> resources/byteman/<rule>.btm
```

`<voltdb>` is an unpacked kit or a built `internal` checkout (anything with
`voltdb/*.jar` and `lib/*.jar`). The script compiles the rule against the
server's classes and catches misspelled classes, methods and fields, wrong
argument types and undeclared exceptions. It exits non-zero on any error.

### 5. Try it on one local node

This step proves the rule fires and that its effect is what you meant,
before a cluster run. Start a single node with the agent, from a kit or a
built `internal` checkout. Use `bin/voltdb`, with a license for enterprise
features such as command logging:

```
BM=~/.m2/repository/org/jboss/byteman/byteman/<version>/byteman-<version>.jar
SUBMIT=~/.m2/repository/org/jboss/byteman/byteman-submit/<version>/byteman-submit-<version>.jar
voltdb init -D /tmp/bm -l license.xml
VOLTDB_OPTS="-javaagent:$BM=listener:true,port:9091,boot:$BM" voltdb start -D /tmp/bm -B
java -cp $SUBMIT org.jboss.byteman.agent.submit.Submit -p 9091 -l resources/byteman/<rule>.btm
# ... trigger the operation with sqlcmd ...
java -cp $SUBMIT org.jboss.byteman.agent.submit.Submit -p 9091        # lists rules; shows where each was injected
java -cp $SUBMIT org.jboss.byteman.agent.submit.Submit -p 9091 -u resources/byteman/<rule>.btm
```

The `fired` lines go to the server's stdout. With `voltdb start -B` that is
`~/.voltdb_server/server.out`. Check volt.log for what the server did. If
a VoltDB is already running on the default ports, give this one other ports
(`--client`, `--admin`, `--internal`, `--zookeeper`, `--replication`,
`--metrics`, `--topicsport`, and `--host=localhost:<internal port>`) and
another listener port.

For a startup rule, add `,script:<rule file>` to the `-javaagent` options
instead of submitting it.

### 6. Add the fault

In `src/jepsen/voltdb/byteman_faults.clj`:

1. Add the fault to `faults`: `{:rule "<file>.btm" :label "<label>"}`.
2. Add a `run-fault` method. It gets the test, the nemesis options and the
   nodes that were up, loads the rule where it belongs, sets up the
   operation the rule targets, unloads the rule (in a `finally`), and
   returns a map. Put anything worth seeing in history.edn into the map.
   Add fault-specific failures to its `:problems` vector, for example
   `(update m :problems (fnil conj []) :not-reported-failed)`.
3. Add the name to `nemeses` in `runner.clj`, and to `rule-set` if it
   belongs in `--nemesis byteman-rules`.

The nemesis does the common part around `run-fault`. It counts the rule's
fired lines on every node before and after, records nodes that were up
before and down after, and adds `:never-fired` and `:nodes-down-after` to
`:problems`. A `run-fault` that cannot run (too few nodes up, say) returns
`{:skipped <reason>}`.

Helpers in the namespace cover the usual steps: `load-rule!`,
`unload-rule!`, `fired-counts` and `fired-since`, `kill-nodes!`,
`start-nodes!` (starting one node of a running cluster rejoins it),
`await-nodes-up`, `await-until`, `running?`, `set-boot-rule!` and
`clear-boot-rule!`. The existing methods are the examples. `promotion-delay`
is a kill and rejoin, `rejoin-kill` uses a startup rule on one node, and
`replay-delay` uses startup rules on every node across a full restart.

### 7. Run it

Locally against your own nodes:

```
lein run test --tarball file://voltdb-ent-<version>.tar.gz -w single --nemesis <fault> --time-limit 300
```

Naming a Byteman fault turns on `--byteman`. On jenkins-st, run
`matrix-systemtest-apprunner-jepsen` for your branch's kit with
`JEPSEN_BRANCH=<your jepsen-volt branch>` and
`JEPSEN_EXTRA_ARGS=--nemesis <fault>`. NEMESIS must be one of the job
script's own names; the last `--nemesis` on the command line wins. Faults
that restart nodes take a minute or two per run, so allow `TIME_LIMIT` of
about two minutes per run you want.

### 8. Read the results

- **Verdict:** the `:byteman` entry in the results map in the Jepsen log
  (or `results.edn`) has the runs per fault, any faults that never ran, and
  every run with problems.
- **Per run:** each fault op in `history.edn` (and the "Byteman <fault>"
  log line) carries `:fired` per node, `:down-after`, `:problems`, the
  victim, and whatever the fault recorded.
- **Server side:** each node's stdout.log has the `BYTEMAN ... fired`
  lines, and volt.log shows what the server did about them.

If the rule fired and a checker failed, you may have the bug. File it with
the plan's triage template (ENG-30420 is an example): the rule file and the
fault are the reproduction.

## Rule cookbook

Patterns the rules in this directory use.

**Delay.** `delay(ms)` sleeps on the calling thread. Remember what else that
thread serves. A network thread carries other connections' responses, and
a site thread blocks its partition (`client-response-delay`,
`promotion-delay`).

**Fire on a sample, or once.** `IF incrementCounter("<label>") % 25 == 0`
fires on every 25th call. `IF flag("<label>")` fires on the first call only,
because `flag` returns true only when it sets the flag. Counters and flags
live in the JVM and survive unloading the rule, so use a fresh label or a
fresh JVM per run (`rejoin-kill`, `replay-delay`).

**Throw so the product code handles it.** A rule's exception skips the
trigger method's own `catch` and `finally` (see [Pitfalls](#pitfalls)). Tag
the thread at the call site, and throw from the callee's entry. The
exception then reaches the product code at its call, like a real one
(`snapshot-write-ioexception`, `replay-error`):

```
RULE <label> tag
CLASS <caller class>
METHOD <caller method>
AT INVOKE <callee method name>
IF <which calls to fail>
DO link("<label>", Thread.currentThread(), "<detail for the log>")
ENDRULE

RULE <label> throw
CLASS <callee's runtime class>
METHOD <callee method>
AT ENTRY
IF linked("<label>", Thread.currentThread()) != null
DO traceln("BYTEMAN <label> fired " + unlink("<label>", Thread.currentThread()));
   throw new java.io.IOException("Byteman <label> (injected)")
ENDRULE
```

A checked exception must be one the callee declares. The callee's runtime
class can be a JDK class (`sun.nio.ch.FileChannelImpl` for
`FileChannel.write`); the agent's `boot:` option makes that work.

**Kill the node.** `killJVM()` halts the JVM immediately, like a power cut:
no shutdown hooks, nothing flushed (`rejoin-kill`).

**Rule in place at startup.** For work that starts with the server, the
fault copies the rule into the node's startup script
(`<base-dir>/byteman/boot.btm`, the agent's `script:` option) before
starting it, and empties it afterwards (`set-boot-rule!`,
`clear-boot-rule!`).

**Limit to your own operation.** Give the operation a recognisable name and
test for it, as `snapshot-write-ioexception` does with the snapshot nonce
(`$0.this$0.m_nonce.startsWith("BYTEMAN_")`). Private fields and the
`this$0` link from an inner class to its outer instance are readable from
rules.

## Byteman reference

The parts of the [rule language](https://downloads.jboss.org/byteman/latest/byteman-programmers-guide.html)
these rules use.

| Element | Meaning |
|---|---|
| `AT ENTRY` / `AT EXIT` | Start of the method, or each return |
| `AT INVOKE <method>` | Just before each call to `<method>` in the trigger method (`AT INVOKE <method> <n>` for the nth) |
| `AT LINE <n>` | A source line; brittle, avoid |
| `$0`, `$1`.. | `this` and the parameters; fields with `$0.m_field` |
| `$!` | The return value, `AT EXIT` |
| `traceln(s)` | Writes a line to the server's stdout |
| `delay(ms)` | Sleeps the calling thread |
| `killJVM()` | Halts the JVM |
| `flag(o)`, `flagged(o)`, `clear(o)` | One-shot flags |
| `incrementCounter(o)`, `readCounter(o)` | Counters |
| `link(map, key, value)`, `linked(map, key)`, `unlink(map, key)` | Values shared between rules, for example per thread |
| `callerEquals(name)` | True when the trigger method's caller is `name` |

## Pitfalls

- **Throwing an exception skips the trigger method's own handlers.** Byteman
  raises a rule's exception from handler code it appends to the trigger
  method, outside that method's try blocks, so the method's own `catch` and
  `finally` never run. Throw from a method it calls instead, as in the
  cookbook. The first snapshot rule threw inside the write task. Its buffers
  leaked, the target was never marked failed, and a debug build crashed on
  the leak.
- **Rules on JDK classes need the agent on the bootstrap classpath.** The
  agent options include `boot:<agent jar>`. Without it, a JDK method with a
  rule loaded cannot load Byteman's classes and throws
  `NoClassDefFoundError` on every call, from every caller. The first version
  of the snapshot rule did this, and a snapshot hung.
- **Anonymous classes are named by source order** (`Foo$3`). A refactor
  silently moves the rule to a different class or none. Type-checking
  catches a class that is gone; the fired-line check catches one that no
  longer runs the code.
- **The work may happen on fewer nodes than you think.** Command log replay
  runs on one host per partition. A replay rule on a random node fired in
  1 run of 4 until it went on every node.
- **Shared threads.** A delay on a network or site thread stalls everything
  else that thread does, which can be the bigger effect.
- **Expected FATALs fail the Jenkins job,** which fails a run on any FATAL
  in the server logs. Keep faults that crash a node on purpose out of
  `byteman-rules`, as `replay-error` is.
- **Unload in a `finally`.** A rule left loaded keeps firing for the rest
  of the test.

Byteman version: pinned in `project.clj` (`org.jboss.byteman/byteman` and
`byteman-submit`).
