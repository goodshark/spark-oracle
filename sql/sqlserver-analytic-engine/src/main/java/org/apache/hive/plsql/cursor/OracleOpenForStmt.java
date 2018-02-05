package org.apache.hive.plsql.cursor;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.cursor.Cursor;

public class OracleOpenForStmt extends BaseStatement {
    OracleCursor cursor;
    TreeNode expressionStmt;

    public void setCursor(OracleCursor c) {
        cursor = c;
    }

    public void setExpr(TreeNode expr) {
        expressionStmt = expr;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public int execute() throws Exception {
        SparkResultSet rs = null;
        if (expressionStmt != null) {
            // TODO query statement in variable
        } else {
            // open cursor
            cursor.setStatus(Cursor.CursorStatus.OPENING);
            TreeNode sqlStmt = cursor.getTreeNode();
            sqlStmt.setExecSession(getExecSession());
            sqlStmt.setAddResult(false);
            sqlStmt.execute();
            rs = (SparkResultSet) sqlStmt.getRs();
        }
        cursor.setRs(rs);
        cursor.setSchema(rs.getColumns());
        // add cursor variable into variable-scope
        addCursor(cursor);
        return 0;
    }
}
