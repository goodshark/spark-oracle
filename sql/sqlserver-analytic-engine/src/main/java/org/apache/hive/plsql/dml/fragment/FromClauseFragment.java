package org.apache.hive.plsql.dml.fragment;

import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by dengrb1 on 6/9 0009.
 */
public class FromClauseFragment extends SqlStatement {
    private SqlStatement sourceFrag = null;

    public void setSourceFrag(SqlStatement stmt) {
        sourceFrag = stmt;
    }

    @Override
    public String getSql() {
        return "";
    }

    @Override
    public String getOriginalSql() {
        return " FROM " + (sourceFrag == null ? "" : sourceFrag.getOriginalSql());
    }

    @Override
    public String getFinalSql() throws Exception {
        return " FROM " + (sourceFrag == null ? "" : sourceFrag.getFinalSql());
    }
}
