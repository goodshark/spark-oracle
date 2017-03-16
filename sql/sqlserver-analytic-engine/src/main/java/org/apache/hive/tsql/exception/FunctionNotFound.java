package org.apache.hive.tsql.exception;

/**
 * Created by zhongdg1 on 2017/1/9.
 */
public class FunctionNotFound extends RuntimeException {
    public FunctionNotFound(String message) {
        super("Procedure or Procedure Not Found # "+message);
    }
}
