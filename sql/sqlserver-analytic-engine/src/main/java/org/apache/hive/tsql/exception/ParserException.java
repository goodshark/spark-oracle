package org.apache.hive.tsql.exception;

/**
 * Created by zhongdg1 on 2017/1/6.
 */
public class ParserException extends Exception {

    public ParserException(String message) {
        super(message);
    }

    @Override
    public String toString() {
        return "Syntax error at line ["+getMessage()+"]";
    }
}
