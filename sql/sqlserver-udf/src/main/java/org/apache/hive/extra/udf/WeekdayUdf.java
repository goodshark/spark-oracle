package org.apache.hive.extra.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hive.tsql.util.DateUtil;

import java.util.Date;

/**
 * Created by zhongdg1 on 2017/4/10.
 */
public class WeekdayUdf extends UDF {
    private static final String PATTERN = "yyyy-MM-dd";

    public String evaluate(String oldDate) {
        try {
            Date left = DateUtil.parseLenient(oldDate, PATTERN);
            return DateUtil.getWeekDay(left);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
