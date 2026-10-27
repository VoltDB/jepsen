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
| `snapshot-write-ioexception.btm` | `DefaultSnapshotDataTarget` write task, table-data block write | `IOException` from `FileChannel.write`, only for snapshots whose nonce starts with `BYTEMAN_` | `--nemesis snapshot-io-error` |

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
- **Check rules before running them.** `TestScript` type-checks a rule file
  against the server's classes, which catches missing fields and methods:

  ```
  java -cp byteman.jar:<voltdb jar>:<voltdb lib/*.jar> org.jboss.byteman.check.TestScript -v resources/byteman/<rule>.btm
  ```

Byteman version: pinned in `project.clj` (`org.jboss.byteman/byteman`).
