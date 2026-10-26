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

Byteman version: pinned in `project.clj` (`org.jboss.byteman/byteman`).
