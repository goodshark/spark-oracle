package org.apache.hive.plsql.cursor;

import org.apache.hive.basesql.cursor.CommonOpenCursorStmt;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.SparkResultSet;

/**
 * Created by dengrb1 on 7/17 0017.
 */
public class OracleOpenCursorStmt extends CommonOpenCursorStmt {
    private static final String STATEMENT_NAME = "_ORACLE_OPEN_CURSOR_";

    public OracleOpenCursorStmt() {
    }

    public OracleOpenCursorStmt(String name, boolean global) {
        super(STATEMENT_NAME, name, global);
    }

    @Override
    public int postExecute() throws Exception {
        storeSchema();
        return 0;
    }

    private void storeSchema() {
        SparkResultSet rs = (SparkResultSet) cursor.getRs();
        ((OracleCursor)cursor).setSchema(rs.getColumns());
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }
}
