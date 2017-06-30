package org.apache.hive.plsql.dml.fragment;

import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.List;

/**
 * Created by wangsm9 on 2017/6/21.
 */
public class FragMentUtils {


    public static String appendSql(List<String> list) {
        StringBuffer sql = new StringBuffer();
        if (null != list && !list.isEmpty()) {
            for (String s : list) {
                sql.append(Common.SPACE);
                sql.append(s);
                sql.append(",");
            }
        }
        return sql.length() > 0 ? sql.substring(0, sql.length() - 1) : "";
    }

    public static String appendFinalSql(List<SqlStatement> list) throws Exception {
        StringBuffer sql = new StringBuffer();
        if (null != list && !list.isEmpty()) {
            for (SqlStatement s : list) {
                sql.append(Common.SPACE);
                sql.append(s.getFinalSql());
                sql.append(",");
            }
        }
        return sql.length() > 0 ? sql.substring(0, sql.length() - 1) : "";
    }

    public static String appendOriginalSql(List<SqlStatement> list)  {
        StringBuffer sql = new StringBuffer();
        if (null != list && !list.isEmpty()) {
            for (SqlStatement s : list) {
                sql.append(Common.SPACE);
                sql.append(s.getOriginalSql());
                sql.append(",");
            }
        }
        return sql.length() > 0 ? sql.substring(0, sql.length() - 1) : "";
    }
}
