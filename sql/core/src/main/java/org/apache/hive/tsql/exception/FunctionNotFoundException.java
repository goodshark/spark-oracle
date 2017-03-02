package hive.tsql.exception;

/**
 * Created by zhongdg1 on 2017/1/20.
 */
public class FunctionNotFoundException extends RuntimeException {
    private String funcName;

    public FunctionNotFoundException(String funcName) {
        this.funcName = funcName;
    }

    @Override
    public String toString() {
        return "Cannot found function # " + funcName;
    }
}
