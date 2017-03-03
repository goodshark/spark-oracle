package org.apache.hive.tsql.udf.math;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.udf.BaseCalculator;

import java.math.RoundingMode;
import java.text.NumberFormat;

/**
 * Created by zhongdg1 on 2017/1/19.
 */
public class RoundCalculator extends BaseCalculator {
    public RoundCalculator() {
        setMinMax(2);
    }

    @Override
    public Var compute() throws Exception {
        Var var = getArguments(0);
        int length = getArguments(1).getInt();
        var.setDataType(Var.DataType.DOUBLE);
//        var.setVarValue(Math.round(var.getDouble()));
        var.setVarValue(round(var.getDouble(), length));
        return var;
    }

    private double round(Double source, int length) {
        if (length < 0) {
            return 0;
        }
        NumberFormat nf = NumberFormat.getNumberInstance();
        nf.setMaximumFractionDigits(length);
        nf.setRoundingMode(RoundingMode.DOWN);
        return Double.valueOf(nf.format(source));
    }
}
