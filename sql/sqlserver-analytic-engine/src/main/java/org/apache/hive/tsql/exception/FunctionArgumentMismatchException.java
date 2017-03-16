package org.apache.hive.tsql.exception;

/**
 * Created by zhongdg1 on 2017/1/19.
 */
public class FunctionArgumentMismatchException extends RuntimeException {
    private String funcName;
    private String needArgument;
    private String realArgument;
    private String argName;

    public FunctionArgumentMismatchException(String funcName, String argName, String needArgument, String realArgument) {
        this.funcName = funcName;
        this.needArgument = needArgument;
        this.realArgument = realArgument;
        this.argName = argName;
    }

    @Override
    public String toString() {
        return "Function[" + funcName + "] argument [" + this.argName + "] must [" + needArgument + "],but now [" + realArgument + "]";
    }
}
