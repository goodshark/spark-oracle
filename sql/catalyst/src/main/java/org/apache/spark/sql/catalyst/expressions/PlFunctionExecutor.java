package org.apache.spark.sql.catalyst.expressions;

/**
 * Created by chenfolin on 2017/8/8.
 */
public interface PlFunctionExecutor {

    public Object eval(Object[] inputdatas);

}
