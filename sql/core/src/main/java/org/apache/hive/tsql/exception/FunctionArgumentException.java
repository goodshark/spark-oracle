package hive.tsql.exception;

/**
 * Created by zhongdg1 on 2017/1/19.
 */
public class FunctionArgumentException extends RuntimeException {
    private String funcName;
    private int size =0;
    private int min =0;
    private int max = 0;

    public FunctionArgumentException(String funcName, int size, int min, int max) {
        this.funcName = funcName;
        this.size = size;
        this.min = min;
        this.max = max;
    }

    @Override
    public String toString() {
        return "Function["+funcName+"] arguments size must between ["+min+"] and ["+max+"], but now ["+size+"]";
    }
}
