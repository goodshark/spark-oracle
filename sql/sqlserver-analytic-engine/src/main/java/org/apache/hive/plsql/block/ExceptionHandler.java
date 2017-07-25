package org.apache.hive.plsql.block;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by dengrb1 on 4/1 0001.
 */

public class ExceptionHandler extends BaseStatement {
    final private String ROOT_EXCEPTION = "others";
    private String exceptionName = "";
    private HashSet<String> exceptionNames = new HashSet<>();
    private TreeNode stmts = null;

    public ExceptionHandler() {
    }

    public void setExceptionName(String name) {
        exceptionNames.add(name.toLowerCase());
    }

    public boolean matchException(String exc) {
        if (exceptionNames.contains(ROOT_EXCEPTION))
            return true;
        return exceptionNames.contains(exc);
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
