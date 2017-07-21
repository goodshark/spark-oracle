package org.apache.hive.plsql.cursor;

import org.apache.hive.basesql.cursor.CommonOpenCursorStmt;
import org.apache.hive.tsql.common.BaseStatement;

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
        // TODO set cursor status into cursor
        return 0;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }
}
