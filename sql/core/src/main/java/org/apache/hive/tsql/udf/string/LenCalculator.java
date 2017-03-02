package org.apache.hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.udf.BaseCalculator;

import java.util.List;

/**
 * Created by zhongdg1 on 2017/1/19.
 */
public class LenCalculator extends BaseCalculator {
    public LenCalculator() {
        setMinMax(1);
    }

    @Override
    public Var compute() {
        return new Var(getArguments(0).getString().length(), Var.DataType.INT);
    }
}
