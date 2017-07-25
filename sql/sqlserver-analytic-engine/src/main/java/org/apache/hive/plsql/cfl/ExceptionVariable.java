package org.apache.hive.plsql.cfl;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

/**
 * Created by dengrb1 on 6/29 0029.
 */
public class ExceptionVariable extends BaseStatement {
    private String exceptionName = "";
    private TreeNode stmts = null;

    public ExceptionVariable() {
    }

    public boolean matchException(String exc) {
        return exceptionName.equalsIgnoreCase(exc);
    }

    public void setStmts(TreeNode block) {
        stmts = block;
    }

    public TreeNode getStmts() {
        return stmts;
    }

    public int execute() throws Exception {
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }
}
