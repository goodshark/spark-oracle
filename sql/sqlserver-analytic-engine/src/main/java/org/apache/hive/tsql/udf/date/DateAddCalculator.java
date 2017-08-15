package org.apache.hive.tsql.udf.date;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.func.DateUnit;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.DateUtil;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by zhongdg1 on 2017/2/8.
 */
public class DateAddCalculator extends BaseCalculator {
    private static final String PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

    public DateAddCalculator() {
        setMinMax(3);
    }

    @Override
    public Var compute() throws Exception {
//        String datePart = getArguments(0).getString(); //只支持天为单位
        DateUnit unit = DateUnit.parse(getArguments(0).getVarValue().toString());
        int number = Double.valueOf(getArguments(1).getString().trim()).intValue();;
        Date oldDate = getArguments(2).setDataType(Var.DataType.DATETIME).getDate();
//        return new Var(new Date(date.getTime()+number*24*60*60*1000L), getArguments(2).getDataType());
        return new Var(doEval(unit, number, oldDate), Var.DataType.DATETIME);
    }

    public String doEval(DateUnit unit, int number, Date left) {
        Calendar leftCal = Calendar.getInstance();
        leftCal.setTime(left);
        int fieldId = 0;
        switch (unit) {
            case MILLISECOND:
                fieldId = Calendar.MILLISECOND;
                break;
            case SECOND:
                fieldId = Calendar.SECOND;
                break;
            case MINUTE:
                fieldId = Calendar.MINUTE;
                break;
            case HOUR:
                fieldId = Calendar.HOUR_OF_DAY;
                break;
            case DAY:
                fieldId = Calendar.DAY_OF_MONTH;
                break;
            case MONTH:
                fieldId = Calendar.MONTH;
                break;
            case YEAR:
                fieldId = Calendar.YEAR;
                break;
            case QUARTER:
                fieldId = Calendar.MONTH;
                number = number * 3;
                break;
            case WEEK:
                fieldId = Calendar.WEEK_OF_YEAR;
                break;
            case DAYOFYEAR:
            case WEEKDAY:
                fieldId = Calendar.DAY_OF_YEAR;
                break;
            default:
                break;
        }
        leftCal.add(fieldId, number);
        return DateUtil.format(leftCal.getTime(), PATTERN);
    }
}
