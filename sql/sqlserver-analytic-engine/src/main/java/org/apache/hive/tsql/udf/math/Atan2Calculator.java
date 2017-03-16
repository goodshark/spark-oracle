package org.apache.hive.tsql.udf.math;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.udf.BaseCalculator;

/**
 * Created by zhongdg1 on 2017/1/19.
 */
public class Atan2Calculator extends BaseCalculator {
    public Atan2Calculator() {
        setMinMax(2);
    }

    @Override
    public Var compute() throws Exception {
        Var var = getArguments(0);
        var.setDataType(Var.DataType.DOUBLE);
        Var var2 = getArguments(1);
        var2.setDataType(Var.DataType.DOUBLE);

        return new Var(Math.atan2(var.getDouble(), var2.getDouble()), Var.DataType.DOUBLE);
    }
}
