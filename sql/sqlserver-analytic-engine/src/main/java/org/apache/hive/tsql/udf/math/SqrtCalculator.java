package org.apache.hive.tsql.udf.math;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.udf.BaseCalculator;

/**
 * Created by zhongdg1 on 2017/1/19.
 */
public class SqrtCalculator extends BaseCalculator {
    public SqrtCalculator() {
        setMinMax(1);
    }

    @Override
    public Var compute() throws Exception {
        Var var = getArguments(0);
        var.setDataType(Var.DataType.DOUBLE);
        var.setVarValue(Math.sqrt(var.getDouble()));
        return var;
    }
}
