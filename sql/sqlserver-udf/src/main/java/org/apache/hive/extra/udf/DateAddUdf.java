package org.apache.hive.extra.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hive.tsql.func.DateUnit;
import org.apache.hive.tsql.udf.date.DateAddCalculator;
import org.apache.hive.tsql.util.DateUtil;

import java.util.Date;

/**
 * Created by zhongdg1 on 2017/4/10.
 */
public class DateAddUdf extends UDF {
    private static final String PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
    private DateAddCalculator calculator = new DateAddCalculator();

    public String evaluate(String part, String number, String oldDate) {
        try {
            DateUnit unit = DateUnit.parse(part);
            Date left = DateUtil.parseLenient(oldDate, PATTERN);
            return calculator.doEval(unit, getInt(number), left);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private int getInt(String number) {
        return Double.valueOf(number.trim()).intValue();
    }


}
