package org.apache.hive.tsql.udf.date;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.func.DateUnit;
import org.apache.hive.tsql.udf.BaseCalculator;

import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by zhongdg1 on 2017/2/8.
 */
public class DateDiffCalculator extends BaseCalculator {

    private static final long SECOND_DIV = 1000L;
    private static final long MINUTE_DIV = 60000L;
    private static final long HOUR_DIV = 3600000L;
    private static final long DAY_DIV = 86400000L;
    private static final long WEEK_DIV = 604800000L;

    public DateDiffCalculator() {
        setMinMax(3);
    }

    @Override
    public Var compute() throws Exception {
        DateUnit unit = DateUnit.parse(getArguments(0).getVarValue().toString());

        Date left = getArguments(1).setDataType(Var.DataType.DATETIME).getDate();
        Date right = getArguments(2).setDataType(Var.DataType.DATETIME).getDate();

        return new Var(doEval(unit, left, right), Var.DataType.LONG);
    }

    public long doEval(DateUnit unit, Date left, Date right) {
        long ret = 0;
//        long leftMillisecond = left.getTime();
//        long rightMillisecond = right.getTime();
        Calendar leftCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        leftCal.setTime(left);
//        leftCal.setFirstDayOfWeek(Calendar.MONDAY);
//        leftCal.setFirstDayOfWeek(Calendar.MONDAY);
        Calendar rightCal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        rightCal.setTime(right);
//        rightCal.setFirstDayOfWeek(Calendar.MONDAY);
//        long millSubRet = leftMillisecond - rightMillisecond;
        switch (unit) {
            case MILLISECOND:
                ret = left.getTime() - right.getTime();
                break;
            case SECOND:
                clearMillSecond(leftCal, rightCal);
                ret = getResult(leftCal, rightCal, (leftCal.getTimeInMillis() - rightCal.getTimeInMillis()), SECOND_DIV, Calendar.SECOND);
                break;
            case MINUTE:
                clearSecond(leftCal, rightCal);
                ret = getResult(leftCal, rightCal, (leftCal.getTimeInMillis() - rightCal.getTimeInMillis()), MINUTE_DIV, Calendar.MINUTE);
                break;
            case HOUR:
                clearMinute(leftCal, rightCal);
                ret = getResult(leftCal, rightCal, (leftCal.getTimeInMillis() - rightCal.getTimeInMillis()), HOUR_DIV, Calendar.HOUR_OF_DAY);
                break;
            case DAY:
                clearHour(leftCal, rightCal);
                ret = getResult(leftCal, rightCal, (leftCal.getTimeInMillis() - rightCal.getTimeInMillis()), DAY_DIV, Calendar.DAY_OF_MONTH);
                break;
            case MONTH:
                ret = getYear(leftCal, rightCal) * 12L + (leftCal.get(Calendar.MONTH) - rightCal.get(Calendar.MONTH));
                break;
            case YEAR:
                ret = getYear(leftCal, rightCal);
                break;
            case QUARTER:
                ret = getYear(leftCal, rightCal) * 4L + (((leftCal.get(Calendar.MONTH) / 3 + 1) - (rightCal.get(Calendar.MONTH) / 3 + 1)));
                break;
            case WEEK:
//                ret = getYear(leftCal, rightCal) * 52L + (leftCal.get(Calendar.WEEK_OF_YEAR) - rightCal.get(Calendar.WEEK_OF_YEAR));
                ret = getYear(leftCal, rightCal) * 52L + getWeekOfYeay(leftCal) - getWeekOfYeay(rightCal);
                break;
            case DAYOFYEAR:
                ret = leftCal.get(Calendar.DAY_OF_YEAR) - rightCal.get(Calendar.DAY_OF_YEAR);
                break;
            default:
                break;
        }
        return ret;
    }

    public int getWeekOfYeay(Calendar cal) {
        int week = cal.get(Calendar.WEEK_OF_YEAR);
        return 53 == week ? 52 : week;
    }

    private void clearSecond(Calendar leftCal, Calendar rightCal) {
        clearMillSecond(leftCal, rightCal);
        leftCal.set(Calendar.SECOND, 0);
        rightCal.set(Calendar.SECOND, 0);
    }

    private void clearHour(Calendar leftCal, Calendar rightCal) {
        clearMinute(leftCal, rightCal);
        leftCal.set(Calendar.HOUR_OF_DAY, 0);
        rightCal.set(Calendar.HOUR_OF_DAY, 0);
    }


    private void clearMinute(Calendar leftCal, Calendar rightCal) {
        clearSecond(leftCal, rightCal);
        leftCal.set(Calendar.MINUTE, 0);
        rightCal.set(Calendar.MINUTE, 0);
    }

    private void clearMillSecond(Calendar leftCal, Calendar rightCal) {
        leftCal.set(Calendar.MILLISECOND, 0);
        rightCal.set(Calendar.MILLISECOND, 0);
    }


    private long getYear(Calendar leftCal, Calendar rightCal) {
        return Long.valueOf(leftCal.get(Calendar.YEAR) - rightCal.get(Calendar.YEAR));
    }

    private long getResult(Calendar leftCal, Calendar rightCal, long millSubRet, long div, int calUnit) {
        long ret;
        ret = millSubRet / div;
        if (0L == ret && (leftCal.get(calUnit) != rightCal.get(calUnit))) {
            ret = millSubRet % div == 0L ? ret : ret + 1;
        }
        return ret;
    }
}
