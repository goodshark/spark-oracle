package org.apache.hive.tsql.exception;

/**
 * Created by dengrb1 on 1/11 0011.
 */
public class CompareException  extends RuntimeException {
    public CompareException(String msg) {
        super(msg);
    }

    @Override
    public String toString() {
        return getMessage();
    }
}
