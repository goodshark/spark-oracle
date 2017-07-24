package org.apache.hive.plsql.dml.fragment;

import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

/**
 * Created by dengrb1 on 6/9 0009.
 */
public class SelectElementFragment extends SqlStatement {
    private ExpressionStatement col = null;
    private String colAlias = "";

    public void setCol(ExpressionStatement c) {
        col = c;
    }

    public void setColAlias(String a) {
        colAlias = a;
    }

    public void replaceAlias(String str) {
        colAlias = str;
    }

    @Override
    public String getSql() {
        String sql = super.getSql();
        if (sql != null)
            return sql;
        sql = col + " " + colAlias;
        super.setSql(sql);
        return sql;
    }

    @Override
    public String getOriginalSql() {
        return colAlias.isEmpty() ? col.getOriginalSql() : col.getOriginalSql() + " " + colAlias;
    }

    @Override
    public String getFinalSql() throws Exception {
        col.setExecSession(getExecSession());
        return colAlias.isEmpty() ? col.getFinalSql() : col.getFinalSql() + " " + colAlias;
    }
}
