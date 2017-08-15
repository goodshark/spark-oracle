package org.apache.hive.tsql.udf.date;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.DateUtil;

import java.util.Calendar;

/**
 * Created by zhongdg1 on 2017/2/8.
 */
public class DateFromPartsCalculator extends BaseCalculator {
    private static final String PATTERN = "yyyy-MM-dd";

    public DateFromPartsCalculator() {
        setMinMax(3);
    }

    @Override
    public Var compute() throws Exception {
        int year = getArguments(0).getInt();
        int month = getArguments(1).getInt();
        int day = getArguments(2).getInt();
        return new Var(doEval(year, month, day), Var.DataType.DATE);
    }

    public String doEval(int year, int month, int day) {
        Calendar cal = Calendar.getInstance();
        cal.set(year, month - 1, day);
        return DateUtil.format(cal.getTime(), PATTERN);
    }
}
