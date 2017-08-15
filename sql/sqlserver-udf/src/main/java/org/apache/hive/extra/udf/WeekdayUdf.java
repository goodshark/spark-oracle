package org.apache.hive.extra.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hive.tsql.util.DateUtil;

import java.util.Calendar;
import java.util.Date;

/**
 * Created by zhongdg1 on 2017/4/10.
 */
public class WeekdayUdf extends UDF {
    private static final String PATTERN = "yyyy-MM-dd";

    public int evaluate(String oldDate) {
        try {
            Date left = DateUtil.parseLenient(oldDate, PATTERN);
            Calendar cal = Calendar.getInstance();
            cal.setTime(left);
            return cal.get(Calendar.DAY_OF_WEEK);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

}
