package org.apache.hive.plsql.cursor;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.cursor.Cursor;

public class OracleOpenForStmt extends BaseStatement {
    OracleCursor cursor;

    public void setCursor(OracleCursor c) {
        cursor = c;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public int execute() throws Exception {
        // open cursor
        cursor.setStatus(Cursor.CursorStatus.OPENING);
        TreeNode sqlStmt = cursor.getTreeNode();
        sqlStmt.setExecSession(getExecSession());
        sqlStmt.setAddResult(false);
        sqlStmt.execute();
        cursor.setRs(sqlStmt.getRs());
        // add cursor variable into variable-scope
        addCursor(cursor);
        return 0;
    }
}
