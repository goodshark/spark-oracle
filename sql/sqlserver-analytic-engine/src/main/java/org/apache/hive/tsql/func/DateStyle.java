package org.apache.hive.tsql.func;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhongdg1 on 2017/4/14.
 */
final public class DateStyle {
    public final Map<String, String> dateStyles = new HashMap<>();

    private DateStyle() {
        dateStyles.put("120", "yyyy-MM-dd HH:mm:ss.SSS");
        dateStyles.put("100", "MM dd yyyy hh:mm a");
        dateStyles.put("101", "MM/dd/yyyy");
        dateStyles.put("102", "yyyy.MM.dd");
        dateStyles.put("103", "dd/MM/yyyy");
        dateStyles.put("104", "dd.MM.yyyy");
        dateStyles.put("105", "dd-MM-yyyy");
        dateStyles.put("106", "dd MM yyyy");
        dateStyles.put("107", "MM dd, yyyy");
        dateStyles.put("108", "hh:mm:ss");
        dateStyles.put("109", "MM dd yyyy hh:mm:ss:SSS a");
        dateStyles.put("110", "MM-dd-yyyy");
        dateStyles.put("111", "yyyy/MM/dd");
        dateStyles.put("112", "yyyyMMdd");
        dateStyles.put("113", "dd MM yyyy HH:mm:ss:SSS");
        dateStyles.put("114", "hh:mm:ss:mmm");
        dateStyles.put("121", "yyyy-MM-dd HH:mm:ss.SSS");
        dateStyles.put("126", "yyyy-MM-ddT hh:mm:ss.SSS");
        dateStyles.put("130", "dd MM yyyy hh:mm:ss:SSS a");
        dateStyles.put("131", "dd/MM/yyyy hh:mm:ss:SSS a");
    }

    public static DateStyle getInstance() {
        return new DateStyle();
    }


    public String getDateStyle(String key) {
        return this.dateStyles.get(key);
    }

}
