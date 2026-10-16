package jepsen.procedures;

import org.voltdb.*;

// Moves amount from one account to another in a single multi-partition
// transaction. Aborts (a definite failure the client can see) if either
// account is missing or, unless negative balances are allowed, if the source
// would go negative.
public class BankTransfer extends VoltProcedure {
  public final SQLStmt read  = new SQLStmt("SELECT balance FROM accounts WHERE id = ?;");
  public final SQLStmt write = new SQLStmt("UPDATE accounts SET balance = ? WHERE id = ?;");

  public long run(long from, long to, long amount, int allowNegative) {
    voltQueueSQL(read, from);
    voltQueueSQL(read, to);
    VoltTable[] r = voltExecuteSQL();
    if (r[0].getRowCount() == 0 || r[1].getRowCount() == 0) {
      throw new VoltAbortException("no such account");
    }
    long fromBalance = r[0].asScalarLong();
    long toBalance   = r[1].asScalarLong();
    if (allowNegative == 0 && fromBalance - amount < 0) {
      throw new VoltAbortException("insufficient funds");
    }
    voltQueueSQL(write, fromBalance - amount, from);
    voltQueueSQL(write, toBalance + amount, to);
    voltExecuteSQL(true);
    return 0;
  }
}
