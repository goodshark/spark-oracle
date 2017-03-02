package hive.tsql.common;

import java.util.List;

/**
 * Created by wangsm9 on 2016/12/7.
 */
public class FunctionBean {

    public enum FunctionType {
        USER_DEFINE, RIGHT, LEFT, BINARY_CHECKSUM, CHECKSUM
    }

    private FunctionType functionType;
    private String functionName;
    private List<Object> args;

    public FunctionType getFunctionType() {
        return functionType;
    }

    public void setFunctionType(FunctionType functionType) {
        this.functionType = functionType;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public List<Object> getArgs() {
        return args;
    }

    public void setArgs(List<Object> args) {
        this.args = args;
    }

    public static void main(String[] args) {
        StringBuffer sb = new StringBuffer();
        Object o = null;
        sb.append(o);
        System.out.println(sb.toString());

    }
}
