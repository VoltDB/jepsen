package jepsen.procedures;

import org.voltdb.*;

// Runs an Elle list-append transaction: parallel arrays of micro-op types
// ("r" or "append"), keys and values. Each key names a list stored as a
// comma-separated string; an append adds the value to the end. Keys may live
// in different partitions, so this is a multi-partition procedure. Every
// micro-op is its own SQL round so a read sees the appends queued before it
// in the same transaction.
public class ListAppendTxn extends VoltProcedure {
  public final SQLStmt read   = new SQLStmt("SELECT value FROM lists WHERE id = ?;");
  public final SQLStmt insert = new SQLStmt("INSERT INTO lists (id, value) VALUES (?, ?);");
  public final SQLStmt update = new SQLStmt("UPDATE lists SET value = ? WHERE id = ?;");

  public VoltTable[] run(String[] fs, long[] ks, long[] vs) {
    assert fs.length == ks.length && ks.length == vs.length;
    VoltTable[] out = new VoltTable[fs.length];
    for (int i = 0; i < fs.length; i++) {
      if (fs[i].equals("r")) {
        voltQueueSQL(read, ks[i]);
        out[i] = voltExecuteSQL()[0];
      } else if (fs[i].equals("append")) {
        voltQueueSQL(read, ks[i]);
        VoltTable current = voltExecuteSQL()[0];
        if (current.getRowCount() == 0) {
          voltQueueSQL(insert, ks[i], Long.toString(vs[i]));
        } else {
          voltQueueSQL(update, current.fetchRow(0).getString(0) + "," + vs[i], ks[i]);
        }
        out[i] = voltExecuteSQL()[0];
      } else {
        throw new IllegalArgumentException("Don't know how to interpret op " + fs[i]);
      }
    }
    return out;
  }
}
