package org.apache.hive.tsql.udf;

import org.apache.hive.tsql.arg.Var;

/**
 * Created by zhongdg1 on 2017/2/6.
 */
public class ConvertCalculator extends BaseCalculator {
    public ConvertCalculator() {
        setMinMax(1);
    }

    @Override
    public Var compute() throws Exception {
        Var var = getArguments(0);
        var.setVarValue(var.getVarValue());
        return var;
    }
}
