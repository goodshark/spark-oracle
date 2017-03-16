package org.apache.hive.tsql.exception;

/**
 * Created by zhongdg1 on 2017/1/9.
 */
public class DatabaseNotFoundException extends RuntimeException {
    public DatabaseNotFoundException(String message) {
        super("Database Not found # "+message);
    }
}
