package org.apache.hive.tsql.udf.math;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.udf.BaseCalculator;

/**
 * Created by zhongdg1 on 2017/1/19.
 */
public class PowerCalculator extends BaseCalculator {
    public PowerCalculator() {
        setMinMax(2);
    }

    @Override
    public Var compute() throws Exception {
        Float val = getArguments(0).getFloat();
        Float val2 = getArguments(1).getFloat();
        return new Var(Math.pow(val, val2), Var.DataType.FLOAT);
    }
}
