package hive.tsql.exception;

/**
 * Created by zhongdg1 on 2017/1/9.
 */
public class NotDeclaredException extends RuntimeException {
    public NotDeclaredException(String message) {
        super(message);
    }

    @Override
    public String toString() {
        return "Not Declared Exception # "+getMessage();
    }
}
