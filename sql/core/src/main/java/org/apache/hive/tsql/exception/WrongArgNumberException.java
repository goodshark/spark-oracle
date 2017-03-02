package org.apache.hive.tsql.exception;

/**
 * Created by dengrb1 on 1/11 0011.
 */
public class WrongArgNumberException extends RuntimeException {
    public WrongArgNumberException(String msg) {
        super(msg);
    }

    @Override
    public String toString() {
        return getMessage() + " operate arg number is wrong";
    }
}
