package org.apache.hive.tsql.exception;

/**
 * Created by zhongdg1 on 2017/1/6.
 */
public class UnsupportedException extends Exception {
    private Position position;

    public UnsupportedException(String message) {
        super(message);
    }

    public UnsupportedException(String message, Position position) {
        super(message);
        this.position = position;
    }

    @Override
    public String toString() {
        return "Not Supported Yet [" + getMessage() + "]" + position;
    }
}
