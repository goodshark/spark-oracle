package org.apache.hive.plsql.dml.fragment;

import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by dengrb1 on 6/9 0009.
 */
public class WhereClauseFragment extends SqlStatement {
    private SqlStatement condition = null;

    public void setCondition(SqlStatement stmt) {
        condition = stmt;
    }

    @Override
    public String getSql() {
        return "";
    }

    @Override
    public String getOriginalSql() {
        return "";
    }

    @Override
    public String getFinalSql() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(" WHERE ");
        condition.setExecSession(getExecSession());
        sb.append(condition.getFinalSql());
        return sb.toString();
    }
}
