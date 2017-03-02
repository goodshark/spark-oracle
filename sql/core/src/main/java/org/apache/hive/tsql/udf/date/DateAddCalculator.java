package org.apache.hive.tsql.udf.date;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.udf.BaseCalculator;

import java.util.Date;

/**
 * Created by zhongdg1 on 2017/2/8.
 */
public class DateAddCalculator extends BaseCalculator {
    public DateAddCalculator() {
        setMinMax(3);
    }

    @Override
    public Var compute() throws Exception {
        String datePart = getArguments(0).getString(); //只支持天为单位
        int number = getArguments(1).getInt();
        Date date = getArguments(2).getDate();
        return new Var(new Date(date.getTime()+number*24*60*60*1000), Var.DataType.DATE);
    }
}
