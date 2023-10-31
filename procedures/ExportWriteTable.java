package jepsen.procedures;

import org.voltdb.*;

// Takes a partition key and an array of longs to insert, and inserts each as a
// row into both a table and a stream.
public class ExportWriteTable extends VoltProcedure {
  public final SQLStmt writeTable = new SQLStmt("INSERT INTO export_table (part, value) VALUES (?, ?);");
 
  // Arrays of the function, key, and value for each op in the transaction.
  // We assume string keys and integer values.
  public long run(int part, long[] elements) {
    for (int i = 0; i < elements.length; i++) {
        voltQueueSQL(writeTable, part, elements[i]);
    }
    voltExecuteSQL(true);
    return 0;
  }
}
