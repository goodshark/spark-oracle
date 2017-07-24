package org.apache.hive.plsql.dml.commonFragment;

import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.spark.launcher.SparkAppHandle;

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

    public static String appendOriginalSql(SqlStatement sqlStatement, ExecSession execSession) {
        sqlStatement.setExecSession(execSession);
        return sqlStatement.getOriginalSql();
    }


    public static String appendFinalSql(SqlStatement sqlStatement, ExecSession execSession) throws Exception {
        StringBuffer sql = new StringBuffer();
        sqlStatement.setExecSession(execSession);
        return sqlStatement.getFinalSql();
    }

    public static String appendFinalSql(List<? extends SqlStatement> list, ExecSession execSession) throws Exception {
        StringBuffer sql = new StringBuffer();
        if (null != list && !list.isEmpty()) {
            for (SqlStatement s : list) {
                sql.append(Common.SPACE);
                s.setExecSession(execSession);
                sql.append(s.getFinalSql());
                sql.append(",");
            }
        }
        return sql.length() > 0 ? sql.substring(0, sql.length() - 1) : "";
    }

    public static String appendOriginalSql(List<? extends SqlStatement> list, ExecSession execSession) {
        StringBuffer sql = new StringBuffer();
        if (null != list && !list.isEmpty()) {
            for (SqlStatement s : list) {
                sql.append(Common.SPACE);
                s.setExecSession(execSession);
                sql.append(s.getOriginalSql());
                sql.append(",");
            }
        }
        return sql.length() > 0 ? sql.substring(0, sql.length() - 1) : "";
    }

}
