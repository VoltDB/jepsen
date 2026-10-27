# Byteman rules

Rule files (`*.btm`) for the `--byteman` option. Setup uploads every file in
this directory to `<base-dir>/byteman/rules/` on each node, next to the agent
jar; `jepsen.voltdb.byteman/load-rules!` and `unload-rules!` load and unload
them by file name while the test runs.

One file per scenario. Each starts with a comment that names the window it
targets (class and method) and the correct behaviour the checkers should see.
Every rule logs when it fires (`traceln("BYTEMAN <rule name> fired")`, which
goes to the server's stdout.log), so a run can prove its fault was injected:
a run where an expected rule never fired counts as a failure, because a
renamed or inlined method would otherwise turn the test into a no-op.

## Rules

| File | Window | Fault | Nemesis |
|---|---|---|---|
| `snapshot-write-ioexception.btm` | `DefaultSnapshotDataTarget` write task, table-data block write | `IOException` from `FileChannel.write`, only for snapshots whose nonce starts with `BYTEMAN_` | `snapshot-io-error` |
| `client-response-delay.btm` | `ClientInterface$ClientResponseWork.getSerializedSize`, after commit | Every 25th response held 1.5 s | `client-response-delay` |
| `promotion-delay.btm` | `SpPromoteAlgo` / `MpPromoteAlgo.repairSurvivors` | 3 s delay before repair, under the deliver lock | `promotion-delay` |
| `rejoin-kill.btm` | `StreamSnapshotSink.processMessage` on the rejoining node | 2 s delay, then `killJVM()` | `rejoin-kill` |
| `replay-delay.btm` | `CommandLogReinitiatorImpl$TaskMessageConsumer.run` on the replaying hosts | 100 ms on each of the first 50 replayed transactions | `replay-delay` |
| `replay-error.btm` | Same, with the throw in `Iv2TransactionCreator.createTransaction` | `RuntimeException` on the 5th replayed transaction | `replay-error` |

To add a fault: write the rule file (header comment, `traceln("BYTEMAN
<label> fired ...")`), add it to `faults` in
`src/jepsen/voltdb/byteman_faults.clj`, add a `run-fault` method that sets
up the operation the rule targets, and add the name to `nemeses` in
`runner.clj`.

## Pitfalls

- **Throwing an exception skips the trigger method's own handlers.** Byteman
  raises a rule's exception from handler code it appends to the trigger
  method, outside that method's try blocks, so the method's own `catch` and
  `finally` never run. To make product code handle the exception as it would
  a real one, throw from a method it calls (the callee's entry), so the
  exception reaches the product code at the call site. A rule on the caller
  can tag the thread (`link`) to say which calls to fail.
- **Rules on JDK classes need the agent on the bootstrap classpath.** The
  agent options include `boot:<agent jar>`. Without it, a JDK method with a
  rule loaded cannot load Byteman's classes and throws `NoClassDefFoundError`
  on every call, from every caller.
- **Anonymous classes are named by source order** (`Foo$3`). A refactor
  silently moves the rule to a different class or none; the fired-line check
  catches that.
- **Rules a JVM needs at startup go in its startup script.** A rejoining or
  recovering node starts the targeted work before a rule could be submitted
  over the listener, so those faults copy the rule to `boot.btm` before
  starting the node and empty it after.
- **Check rules before running them.** `TestScript` type-checks a rule file
  against the server's classes, which catches missing fields and methods:

  ```
  java -cp byteman.jar:<voltdb jar>:<voltdb lib/*.jar> org.jboss.byteman.check.TestScript -v resources/byteman/<rule>.btm
  ```

Byteman version: pinned in `project.clj` (`org.jboss.byteman/byteman`).
