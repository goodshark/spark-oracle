package org.apache.hive.tsql.exception;

/**
 * Created by zhongdg1 on 2017/1/9.
 */
public class AlreadyDeclaredException extends RuntimeException {
    private Position position;

    public AlreadyDeclaredException(String message) {
        super(message);
    }

    public AlreadyDeclaredException(String message, Position position) {
        super(message);
        this.position = position;
    }


    @Override
    public String toString() {
        return "Already declared # ["+getMessage()+"]"+position;
    }
}
