package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by dengrb1 on 6/9 0009.
 */
public class WhereClauseFragment extends SqlStatement {
    private SqlStatement condition ;

    public void setCondition(SqlStatement stmt) {
        condition = stmt;
    }

    @Override
    public String getSql() {
        return "";
    }

    @Override
    public String getOriginalSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(" WHERE ");
        condition.setExecSession(getExecSession());
        sb.append(condition.getOriginalSql());
        return sb.toString();
    }

}
