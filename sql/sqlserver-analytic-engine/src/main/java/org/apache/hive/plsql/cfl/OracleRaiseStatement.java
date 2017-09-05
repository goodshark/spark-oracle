package org.apache.hive.plsql.cfl;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

/**
 * Created by dengrb1 on 6/26 0026.
 */
public class OracleRaiseStatement extends BaseStatement {
    private final String RUN_TIME_EXCEPTION = "$RUNTIME_ERROR_*";
    private String exceptionName = "";
    private String exceptionInfo = "";

    public OracleRaiseStatement() {
    }

    public OracleRaiseStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void setExceptionName(String exc) {
        exceptionName = exc;
    }

    public String getExceptionName() {
        return exceptionName;
    }

    public void setRunTimeException() {
        exceptionName = RUN_TIME_EXCEPTION;
    }

    public boolean isRunTimeException() {
        return exceptionName.equals(RUN_TIME_EXCEPTION);
    }

    public void setExceptionInfo(String str) {
        exceptionInfo = str;
    }

    public String getExceptionInfo() {
        return exceptionInfo;
    }

    @Override
    public int execute() throws Exception {
        return 0;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }
}
