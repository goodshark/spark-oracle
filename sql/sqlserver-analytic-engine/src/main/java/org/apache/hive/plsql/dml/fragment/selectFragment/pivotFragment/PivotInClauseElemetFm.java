package org.apache.hive.plsql.dml.fragment.selectFragment.pivotFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnAliasFragment;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/6.
 * <p>
 * pivot_in_clause_element
 * : pivot_in_clause_elements column_alias?
 */
public class PivotInClauseElemetFm extends SqlStatement {

    private PivotInClauseElemetsFm pivotInClauseElemetsFm;
    private ColumnAliasFragment columnAliasFragment;

    public PivotInClauseElemetsFm getPivotInClauseElemetsFm() {
        return pivotInClauseElemetsFm;
    }

    public void setPivotInClauseElemetsFm(PivotInClauseElemetsFm pivotInClauseElemetsFm) {
        this.pivotInClauseElemetsFm = pivotInClauseElemetsFm;
    }

    public ColumnAliasFragment getColumnAliasFragment() {
        return columnAliasFragment;
    }

    public void setColumnAliasFragment(ColumnAliasFragment columnAliasFragment) {
        this.columnAliasFragment = columnAliasFragment;
    }
}
