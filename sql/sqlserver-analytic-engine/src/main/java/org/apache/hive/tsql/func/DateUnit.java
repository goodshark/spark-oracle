package org.apache.hive.tsql.func;

public enum DateUnit {
    YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, MILLISECOND, QUARTER, DAYOFYEAR, WEEK, WEEKDAY;

    public static DateUnit parse(String value) {
        value = value.trim().toLowerCase();
        if ("yyyy".equals(value) || "year".equals(value) || "yy".equals(value)) {
            return YEAR;
        }

        if ("quarter".equals(value) || "q".equals(value) || "qq".equals(value)) {
            return QUARTER;
        }
        if ("month".equals(value) || "m".equals(value) || "mm".equals(value)) {
            return MONTH;
        }
        if ("dayofyear".equals(value) || "dy".equals(value) || "y".equals(value)) {
            return DAYOFYEAR;
        }
        if ("day".equals(value) || "d".equals(value) || "dd".equals(value)) {
            return DAY;
        }

        if ("week".equals(value) || "wk".equals(value) || "ww".equals(value)) {
            return WEEK;
        }

        if ("weekday".equals(value) || "dw".equals(value) || "w".equals(value)) {
            return WEEKDAY;
        }

        if ("hour".equals(value) || "hh".equals(value)) {
            return HOUR;
        }

        if ("minute".equals(value) || "mi".equals(value) || "n".equals(value)) {
            return MINUTE;
        }

        if ("second".equals(value) || "ss".equals(value) || "s".equals(value)) {
            return SECOND;
        }

        if ("second".equals(value) || "ss".equals(value) || "s".equals(value)) {
            return SECOND;
        }

        if ("millisecond".equals(value) || "ms".equals(value)) {
            return MILLISECOND;
        }
        return null;
    }
}