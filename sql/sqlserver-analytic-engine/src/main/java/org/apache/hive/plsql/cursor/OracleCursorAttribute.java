package org.apache.hive.plsql.cursor;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.dml.ExpressionStatement;

/**
 * Created by dengrb1 on 7/25 0025.
 */
public class OracleCursorAttribute extends ExpressionStatement {
    private String cursorName = "";
    private String mark = "";
    private Var result = new Var();

    public void setCursorName(String name) {
        cursorName = name;
    }

    public void setMark(String m) {
        mark = m;
    }

    private void generateResult(OracleCursor cursor) throws Exception {
        switch (mark.toUpperCase()) {
            case "%ISOPEN":
                result.setDataType(Var.DataType.BOOLEAN);
                result.setVarValue(cursor.isOpen());
                break;
            case "%FOUND":
                result.setDataType(Var.DataType.BOOLEAN);
                result.setVarValue(cursor.hasMoreRows());
                break;
            case "%NOTFOUND":
                result.setDataType(Var.DataType.BOOLEAN);
                result.setVarValue(!cursor.hasMoreRows());
                break;
            case "%ROWCOUNT":
                result.setDataType(Var.DataType.INTEGER);
                result.setVarValue(cursor.getCurrentRowCount());
                break;
        }
        setRs(new SparkResultSet().addRow(new Object[] {result}));
    }

    @Override
    public int execute() throws Exception {
        OracleCursor cursor = (OracleCursor) findCursor(cursorName, false);
        if (cursor == null)
            throw new Exception("can not find cursor " + cursorName);
        generateResult(cursor);
        return 0;
    }
}
