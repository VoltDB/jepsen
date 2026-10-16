package jepsen.procedures;

import org.voltdb.*;

// Runs an Elle read/write-register transaction: parallel arrays of micro-op
// types ("r" or "w"), keys and values. Keys may live in different partitions,
// so this is declared as a multi-partition procedure. Reads return one row
// with the register's value, or no rows if the key was never written.
public class RWRegisterTxn extends VoltProcedure {
  public final SQLStmt read  = new SQLStmt("SELECT value FROM rw_registers WHERE id = ?;");
  public final SQLStmt write = new SQLStmt("UPSERT INTO rw_registers (id, value) VALUES (?, ?);");

  public VoltTable[] run(String[] fs, long[] ks, long[] vs) {
    assert fs.length == ks.length && ks.length == vs.length;
    for (int i = 0; i < fs.length; i++) {
      if (fs[i].equals("r")) {
        voltQueueSQL(read, ks[i]);
      } else if (fs[i].equals("w")) {
        voltQueueSQL(write, ks[i], vs[i]);
      } else {
        throw new IllegalArgumentException("Don't know how to interpret op " + fs[i]);
      }
    }
    return voltExecuteSQL(true);
  }
}
