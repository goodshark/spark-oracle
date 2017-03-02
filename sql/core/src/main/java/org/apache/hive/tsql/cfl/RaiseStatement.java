package hive.tsql.cfl;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.exception.UnsupportedException;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class RaiseStatement extends BaseStatement {
    private String msgStr = null;
    private String severityStr = null;
    private String stateStr = null;
    private List<Var> argList = new ArrayList<Var>();

    private String exceptionStr = "";

    public RaiseStatement() {
    }

    public RaiseStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void setMsgStr(String msg) {
        msgStr = msg;
    }

    public void setSeverityStr(String s) {
        severityStr = s;
    }

    public void setStateStr(String s) {
        stateStr = s;
    }

    public void appendArg(Var arg) {
        argList.add(arg);
    }

    private String parseMsg() throws Exception {
        if (msgStr == null)
            return "null msg";
        if (msgStr.startsWith("@")) {
            Var var = findVar(msgStr);
            return var.getVarValue().toString();
        } else {
            return msgStr.toString();
        }
    }

    private String parseSeverity() throws Exception {
        if (severityStr == null)
            throw new UnsupportedException("raise error stmt severity is null");
        try {
            int severityNum = 0;
            if (severityStr.startsWith("@")) {
                Var var = findVar(severityStr);
                severityNum = Integer.parseInt(var.getVarValue().toString());
            } else {
                severityNum = Integer.parseInt(severityStr.toString());
            }
            if (severityNum < 0) {
                severityNum = 0;
            } else if (severityNum > 25) {
                severityNum = 25;
            }
            return Integer.toString(severityNum);
        } catch (Exception e) {
            throw new Exception("raise error stmt severity is not number");
        }
    }

    private String parseState() throws Exception {
        if (stateStr == null)
            throw new UnsupportedException("raise error stmt state is null");
        try {
            int stateNum;
            if (stateStr.startsWith("@")) {
                Var var = findVar(stateStr);
                stateNum = Integer.parseInt(var.getVarValue().toString());
            } else {
                stateNum = Integer.parseInt(stateStr.toString());
            }
            if (stateNum < 0) {
                stateNum = 0;
            } else if (stateNum > 255) {
                stateNum = 255;
            }
            return Integer.toString(stateNum);
        } catch (Exception e) {
            throw new Exception("raise error stmt state is not number");
        }
    }

    private Object getRealArg(Var var) throws ParseException {
        Var.DataType varType = var.getDataType();
        String varVal = var.getVarValue().toString();
        if (varType == Var.DataType.STRING) {
            varVal = varVal.trim();
            if (varVal.startsWith("\'") && varVal.endsWith("\'")) {
                return varVal.substring(1, varVal.length()-1);
            } else {
                return varVal;
            }
        } else if (varType == Var.DataType.INT || varType == Var.DataType.LONG) {
            return Integer.parseInt(varVal);
        } else if (varType == Var.DataType.FLOAT) {
            return Float.parseFloat(varVal);
        } else {
            return varVal;
        }
    }

    private String parseArg() throws Exception {
        String formatStr = parseMsg();
        Object[] objArgArray = new Object[argList.size()];
        for (int i = 0; i < argList.size(); i++) {
            Var var = argList.get(i);
            Var.DataType varType = var.getDataType();
            String varVal = var.getVarValue().toString();
            // substitute the var, if var is a variable
            if (varType == Var.DataType.STRING && varVal.startsWith("@")) {
                var = findVar(varVal);
            }
            objArgArray[i] = getRealArg(var);
        }
        return String.format(formatStr, objArgArray);
    }

    public String getExceptionStr() {
        return exceptionStr;
    }

    public int execute() throws Exception {
        String severityNumStr = parseSeverity();
        String stateNumStr = parseState();
        String errorMsg = parseArg();
        exceptionStr = errorMsg + ", severity: " + severityNumStr + ", state: " + stateNumStr;
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }
}
