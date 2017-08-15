package org.apache.hive.tsql.udf;

import org.apache.hive.tsql.arg.Var;

/**
 * Created by wangsm9 on 2017/4/26.
 */
public class ErrorNumberCalculator extends BaseCalculator {
    @Override
    public Var compute() throws Exception {
        return new Var("0", Var.DataType.INT);
    }
}
