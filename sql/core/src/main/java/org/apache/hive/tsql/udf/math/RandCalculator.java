package org.apache.hive.tsql.udf.math;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.udf.BaseCalculator;

import java.util.Random;

/**
 * Created by zhongdg1 on 2017/1/19.
 */
public class RandCalculator extends BaseCalculator {
    public RandCalculator() {
        setMinSize(0);
        setMaxSize(1);
    }

    @Override
    public Var compute() throws Exception {
        int size = getSize();
        Var var = 1 == size ? getArguments(0) : null;
        if (null == var) {
            return new Var(Math.random(), Var.DataType.DOUBLE);
        }
        var.setVarValue(new Random(Long.valueOf(var.getVarValue().toString())).nextDouble());
        var.setDataType(Var.DataType.DOUBLE);
        return var;
    }


}
