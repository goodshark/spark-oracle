package org.apache.hive.tsql.util;

import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * Created by zhongdg1 on 2016/12/28.
 */
public class StrUtils {
    private final static String COMMA = ",";

    public static String concat(List<String> list) {
        if (null == list || list.isEmpty()) {
            return null;
        }
        StringBuffer sb = new StringBuffer();
        for (String str : list) {
            if (sb.length() != 0) {
                sb.append(COMMA);
            }
            sb.append(str);
        }
        return sb.toString();
    }

    public static String concatAddQuot(List<String> list) {
        if (null == list || list.isEmpty()) {
            return null;
        }
        StringBuffer sb = new StringBuffer();
        for (String str : list) {
            if (sb.length() != 0) {
                sb.append(COMMA);
            }
            sb.append("'" + str + "'");
        }
        return sb.toString();
    }

    public static String trimQuot(String str) {
        if (StringUtils.isBlank(str))
            return str;
        if (str.trim().startsWith("'") && str.trim().endsWith("'"))
            str = str.trim();
        else
            return str;
        if (str.startsWith("'") && str.endsWith("'")) {
            return str.substring(1, str.length() - 1);
        }
        return str;
    }

    public static String addQuot(String str) {
        if (str.startsWith("'") && str.endsWith("'")) {
            return str;
        } else {
            return "'" + str + "'";
        }


    }


    public static String trimRight(String str) {
        if (StringUtils.isBlank(str)) {
            return "";
        }
        int index = 0;
        for (int i = str.length() - 1; i >= 0; i--) {
            char c = str.charAt(i);
            if (c != ' ') {
                index = i;
                break;
            }
        }
        return str.substring(0, index + 1);
    }

    public static String trimBracket(String s) {
        String rs = s.trim();
        if (s.startsWith("[")) {
            rs = s.substring(1, s.length());
        }
        if (s.endsWith("]")) {
            rs = rs.substring(0, rs.length() - 1);
        }
        return rs;
    }

    public static String replaceAllBracket(String s) {
        return s.replaceAll("]", "").replaceAll("\\[", "");
    }

    public static String replaceAllBracketToQuit(String s) {
        String rs = s;
        if (s.startsWith("[") && s.endsWith("]")) {
            rs = "`" + s.substring(1, s.length() - 2) + "`";
        } else {
            rs = "`" + s + "`";
        }
        return rs;
    }


    public static String addBackQuote(String s) {
        return "`" + s.trim().replaceAll("`", "") + "`";
    }


}
