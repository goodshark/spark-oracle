package org.apache.hive.extra.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hive.tsql.udf.date.DateFromPartsCalculator;

/**
 * Created by zhongdg1 on 2017/4/10.
 */
public class DateFromPartsUdf extends UDF {

    private DateFromPartsCalculator calculator = new DateFromPartsCalculator();

    public String evaluate(int year, int month, int day) {
        try {
            return calculator.doEval(year, month, day);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
