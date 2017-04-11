package org.apache.hive.extra.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by zhongdg1 on 2017/4/10.
 */
public class DateDiffUdf extends UDF {
    private static final String PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final long SECOND_DIV = 1000L;
    private static final long MINUTE_DIV = 60000L;
    private static final long HOUR_DIV = 3600000L;
    private static final long DAY_DIV = 86400000L;
    private static final long WEEK_DIV = 604800000L;


    public long evaluate(String part, String first, String second) {
        long ret = 0;
        try {
            DateUnit unit = DateUnit.parse(part);
            Date left = DateUtil.parseLenient(first, PATTERN);
            Date rigth = DateUtil.parseLenient(second, PATTERN);
            long leftMillisecond = left.getTime();
            long rightMillisecond = rigth.getTime();
            Calendar leftCal = Calendar.getInstance();
            leftCal.setTime(left);
            Calendar rigthCal = Calendar.getInstance();
            rigthCal.setTime(rigth);
            long millSubRet = leftMillisecond - rightMillisecond;
            switch (unit) {
                case MILLISECOND:
                    ret = millSubRet;
                    break;
                case SECOND:
                    ret = getResult(leftCal, rigthCal, millSubRet, SECOND_DIV, Calendar.SECOND);
                    break;
                case MINUTE:
                    ret = getResult(leftCal, rigthCal, millSubRet, MINUTE_DIV, Calendar.MINUTE);
                    break;
                case HOUR:
                    ret = getResult(leftCal, rigthCal, millSubRet, HOUR_DIV, Calendar.HOUR_OF_DAY);
                    break;
                case DAY:
                    ret = getResult(leftCal, rigthCal, millSubRet, DAY_DIV, Calendar.DAY_OF_MONTH);
                    break;
                case MONTH:
                    ret = getYear(leftCal, rigthCal) * 12L + (leftCal.get(Calendar.MONTH) - rigthCal.get(Calendar.MONTH));
                    break;
                case YEAR:
                    ret = getYear(leftCal, rigthCal);
                    break;
                case QUARTER:
                    ret = getYear(leftCal, rigthCal) * 4L + (((leftCal.get(Calendar.MONTH) / 3 + 1) - (rigthCal.get(Calendar.MONTH) / 3 + 1)));
                    break;
                case WEEK:
                    ret = getYear(leftCal, rigthCal) * 52L + (leftCal.get(Calendar.WEEK_OF_YEAR) - rigthCal.get(Calendar.WEEK_OF_YEAR));
//                    ret = getResult(leftCal, rigthCal, millSubRet, WEEK_DIV, Calendar.WEEK_OF_YEAR);
//                    if (ret == 0L) {
//                        ret = Long.valueOf(leftCal.get(Calendar.WEEK_OF_YEAR) - rigthCal.get(Calendar.WEEK_OF_YEAR));
//                    }
                    break;
                default:
                    break;
        }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return ret;
    }

    private long getYear(Calendar leftCal, Calendar rigthCal) {
        return Long.valueOf(leftCal.get(Calendar.YEAR) - rigthCal.get(Calendar.YEAR));
    }

    private long getResult(Calendar leftCal, Calendar rigthCal, long millSubRet, long div, int calUnit) {
        long ret;
        ret = millSubRet / div;
        if (0L == ret && (leftCal.get(calUnit) != rigthCal.get(calUnit))) {
            ret = millSubRet % div == 0L ? ret : ret + 1;
        }
        return ret;
    }

    public long evaluate(String first, String second) {
        return evaluate("day", first, second);
    }
}
