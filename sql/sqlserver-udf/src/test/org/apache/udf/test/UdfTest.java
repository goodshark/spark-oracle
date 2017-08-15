package org.apache.udf.test;

import org.apache.hive.extra.udf.MillisecondUdf;
import org.apache.hive.extra.udf.RightUdf;
import org.apache.hive.extra.udf.WeekdayUdf;

/**
 * Created by zhongdg1 on 2017/4/10.
 */
public class UdfTest {
    public static void main(String[] args) {
//        DateDiffUdf udf = new DateDiffUdf();
//        System.out.println(udf.evaluate("second", "2017-04-10 14:29:51.490", "2017-04-10 14:29:51.390"));
//        System.out.println(udf.evaluate("second" ,"2017-12-31 14:30:01.001","2017-12-31 14:30:02.002"));
//        System.out.println(udf.evaluate("month" ,"2018-12-11 1:0:1.111","2018-1-11 1:0:1.234"));
//        DateAddUdf udf = new DateAddUdf();
//        System.out.println(udf.evaluate("second", "2017-04-10 14:29:51.490", "2017-04-10 14:29:51.390"));
//        System.out.println(udf.evaluate("second" ,"2017-12-31 14:30:01.001","2017-12-31 14:30:02.002"));
//        System.out.println(udf.evaluate("yy" ,1, "'2016-05-01 01:02:05.747'"));
//        DateFromPartsCalculator calculator = new DateFromPartsCalculator();
//        System.out.println(calculator.doEval(2017, 5, 4));
//        System.out.println(udf.evaluate("dd" ,"2017-04-14 19:15:15.647","2017-04-16 10:15:15.647"));
//        System.out.println(new WeekdayUdf().evaluate("2017-01-01 01:02:05.747"));
//        System.out.println(new MillisecondUdf().evaluate("2017-01-01 01:02:05.747"));

        RightUdf udf = new RightUdf();
        System.out.println(udf.evaluate("abcd", 5));

    }
}
