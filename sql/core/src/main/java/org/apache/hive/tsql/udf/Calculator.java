package org.apache.hive.tsql.udf;

import org.apache.hive.tsql.arg.Var;

/**
 * Created by zhongdg1 on 2017/1/19.
 */
public interface Calculator {

    Var compute() throws Exception;
}
