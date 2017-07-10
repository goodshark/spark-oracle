package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnAliasFragment;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

/**
 * Created by dengrb1 on 6/9 0009.
 */
public class SelectElementFragment extends SqlStatement {
    private ExpressionStatement col = null;
    private ColumnAliasFragment colAlias = null;

    public void setCol(ExpressionStatement c) {
        col = c;
    }

    public void setColAlias(ColumnAliasFragment a) {
        colAlias = a;
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
        return col.getOriginalSql() + " " + colAlias.getOriginalSql();
    }

    @Override
    public String getFinalSql() throws Exception {
        col.setExecSession(getExecSession());
        return  col.getFinalSql() + " " + colAlias.getFinalSql();
    }
}
