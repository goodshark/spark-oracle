package org.apache.hive.plsql.cursor;

import org.apache.hive.basesql.cursor.CommonCloseCursorStmt;
import org.apache.hive.tsql.common.BaseStatement;

/**
 * Created by dengrb1 on 7/17 0017.
 */
public class OracleCloseCursorStmt extends CommonCloseCursorStmt {
    private static final String STATEMENT_NAME = "_ORACLE_CLOSE_CURSOR_";

    public OracleCloseCursorStmt() {
    }

    public OracleCloseCursorStmt(String name, boolean global) {
        super(STATEMENT_NAME, name, global);
    }
}
