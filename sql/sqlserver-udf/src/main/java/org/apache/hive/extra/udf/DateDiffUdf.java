package org.apache.hive.extra.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hive.tsql.func.DateUnit;
import org.apache.hive.tsql.udf.date.DateDiffCalculator;
import org.apache.hive.tsql.util.DateUtil;

import java.util.Date;

/**
 * Created by zhongdg1 on 2017/4/10.
 */
public class DateDiffUdf extends UDF {

    private static final String PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

    public long evaluate(String part, String first, String second) {
        try {
            DateUnit unit = DateUnit.parse(part);
            Date left = DateUtil.parseLenient(first, PATTERN);
            Date right = DateUtil.parseLenient(second, PATTERN);
            return new DateDiffCalculator().doEval(unit, left, right);

        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }


    public long evaluate(String first, String second) {
        return evaluate("day", first, second);
    }
}
