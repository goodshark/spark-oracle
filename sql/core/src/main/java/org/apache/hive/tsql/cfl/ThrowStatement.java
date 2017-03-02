package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.exception.UnsupportedException;
import org.apache.hive.tsql.common.TreeNode;
import scala.Int;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class ThrowStatement extends BaseStatement {
    private String msg = null;
    private String errorNumStr = null;
    private String stateNumStr = null;

    private String errorMsg = null;
    private int errorNum = -1;
    private int stateNum = -1;

    private String throwExeceptionStr = null;

    public ThrowStatement() {
    }

    public ThrowStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void setMsg(String message) {
        msg = message;
    }

    public void setErrorNumStr(String numStr) {
        errorNumStr = numStr;
    }

    public void setStateNumStr(String numStr) {
        stateNumStr = numStr;
    }

    private void parseErrorNum() throws Exception {
        if (errorNumStr.startsWith("@")) {
            Var errorN = findVar(errorNumStr);
            errorNum = Integer.parseInt(errorN.getVarValue().toString());
        } else {
            errorNum = Integer.parseInt(errorNumStr);
        }
    }

    private void parseStateNum() throws Exception {
        if (stateNumStr.startsWith("@")) {
            Var stateN = findVar(stateNumStr);
            stateNum = Integer.parseInt(stateN.getVarValue().toString());
        } else {
            stateNum = Integer.parseInt(stateNumStr);
        }
    }

    public String getThrowExeceptionStr() {
        return throwExeceptionStr;
    }

    public int execute() throws Exception {
        errorMsg = msg;
        if (msg.startsWith("@")) {
            Var msgVar = findVar(msg);
            if (msgVar.getDataType() != Var.DataType.STRING)
                throw new UnsupportedException("throw stmt arg msg is not string type");
            errorMsg = msgVar.getVarValue().toString();
        }
        try {
            parseErrorNum();
            parseStateNum();
        } catch (Exception e) {
            throw new UnsupportedException("throw stmt arg error num is not number variable or number");
        }
        if (errorNum < 50000 || errorNum > 2147483647) {
            throw new UnsupportedException("throw stmt arg error num is out of range (50000 - 2147483647)");
        }
        if (stateNum < 0 || stateNum > 255) {
            throw new UnsupportedException("throw stmt arg state num is out of range (0 - 255)");
        }
        throwExeceptionStr = "error num: " + errorNum + ", " + errorMsg + ", state num: " + stateNum;
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }
}
