package org.apache.hive.tsql.udf.date;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.udf.BaseCalculator;

import java.util.Date;

/**
 * Created by zhongdg1 on 2017/2/8.
 */
public class DateDiffCalculator extends BaseCalculator {
    public DateDiffCalculator() {
        setMinMax(3);
    }

    @Override
    public Var compute() throws Exception {
        String datePart = getArguments(0).getString(); //只支持天为单位

        Date left = getArguments(1).setDataType(Var.DataType.DATE).getDate();
        Date right = getArguments(2).setDataType(Var.DataType.DATE).getDate();

        return new Var((right.getTime()-left.getTime())/(24*60*60*1000L), Var.DataType.INT);
    }
}
