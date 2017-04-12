package org.apache.udf.test;

import org.apache.hive.extra.udf.DateDiffUdf;

/**
 * Created by zhongdg1 on 2017/4/10.
 */
public class UdfTest {
    public static void main(String[] args) {
        DateDiffUdf udf = new DateDiffUdf();
//        System.out.println(udf.evaluate("second", "2017-04-10 14:29:51.490", "2017-04-10 14:29:51.390"));
//        System.out.println(udf.evaluate("second" ,"2017-12-31 14:30:01.001","2017-12-31 14:30:02.002"));
        System.out.println(udf.evaluate("day" ,"2018-12-11 1:0:1.111","2018-1-11 1:0:1.234"));
    }
}
