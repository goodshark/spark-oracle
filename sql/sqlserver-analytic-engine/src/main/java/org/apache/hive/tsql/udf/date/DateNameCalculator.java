package org.apache.hive.tsql.udf.date;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.func.DateUnit;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.DateUtil;

import java.util.Date;

/**
 * Created by zhongdg1 on 2017/2/8.
 */
public class DateNameCalculator extends BaseCalculator {
    public DateNameCalculator() {
        setMinMax(3);
    }

    @Override
    public Var compute() throws Exception {
        DateUnit dateUnit = (DateUnit) getArguments(0).getVarValue();
        Date date = getArguments(1).getDate();
        if (dateUnit == DateUnit.WEEKDAY && "DATENAME".equalsIgnoreCase(getArguments(2).getString())) {
            return new Var(DateUtil.getWeekDay(date), Var.DataType.STRING);
        }
        return new Var(getDatePartValue(dateUnit, date), Var.DataType.INT);
    }


}
