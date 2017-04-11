package org.apache.hive.tsql.udf.date;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.func.DateUnit;
import org.apache.hive.tsql.udf.BaseCalculator;

import java.util.Calendar;
import java.util.Date;

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
        long leftMillisecond = left.getTime();
        long rightMillisecond = right.getTime();
        Calendar leftCal = Calendar.getInstance();
        leftCal.setTime(left);
        Calendar rightCal = Calendar.getInstance();
        rightCal.setTime(right);
        long millSubRet = leftMillisecond - rightMillisecond;
        switch (unit) {
            case MILLISECOND:
                ret = millSubRet;
                break;
            case SECOND:
                ret = getResult(leftCal, rightCal, millSubRet, SECOND_DIV, Calendar.SECOND);
                break;
            case MINUTE:
                ret = getResult(leftCal, rightCal, millSubRet, MINUTE_DIV, Calendar.MINUTE);
                break;
            case HOUR:
                ret = getResult(leftCal, rightCal, millSubRet, HOUR_DIV, Calendar.HOUR_OF_DAY);
                break;
            case DAY:
                ret = getResult(leftCal, rightCal, millSubRet, DAY_DIV, Calendar.DAY_OF_MONTH);
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
                ret = getYear(leftCal, rightCal) * 52L + (leftCal.get(Calendar.WEEK_OF_YEAR) - rightCal.get(Calendar.WEEK_OF_YEAR));
//                    ret = getResult(leftCal, rigthCal, millSubRet, WEEK_DIV, Calendar.WEEK_OF_YEAR);
//                    if (ret == 0L) {
//                        ret = Long.valueOf(leftCal.get(Calendar.WEEK_OF_YEAR) - rigthCal.get(Calendar.WEEK_OF_YEAR));
//                    }
                break;
            default:
                break;
        }
        return ret;
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
