package org.apache.hive.extra.udf;

/**
 * Created by zhongdg1 on 2017/4/10.
 */
public class UdfTest {
    public static void main(String[] args) {
        DateDiffUdf udf = new DateDiffUdf();
//        System.out.println(udf.evaluate("second", "2017-04-10 14:29:51.490", "2017-04-10 14:29:51.390"));
//        System.out.println(udf.evaluate("second" ,"2017-12-31 14:30:01.001","2017-12-31 14:30:02.002"));
        System.out.println(udf.evaluate("qq" ,"2016-16-31 13:32:01.001","2018-1-1 1:1:02.002"));
    }
}
