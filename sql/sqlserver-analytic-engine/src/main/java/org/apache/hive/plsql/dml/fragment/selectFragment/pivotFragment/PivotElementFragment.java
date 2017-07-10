package org.apache.hive.plsql.dml.fragment.selectFragment.pivotFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnAliasFragment;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

/**
 * Created by wangsm9 on 2017/7/6.
 * pivot_element
 * : aggregate_function_name '(' expression ')' column_alias?
 * ;
 */
public class PivotElementFragment extends SqlStatement {

    private AggregateFunctionNameFm aggregateFunctionNameFm;
    private ExpressionStatement expressionStatement;
    private ColumnAliasFragment columnAliasFragment;

    public AggregateFunctionNameFm getAggregateFunctionNameFm() {
        return aggregateFunctionNameFm;
    }

    public void setAggregateFunctionNameFm(AggregateFunctionNameFm aggregateFunctionNameFm) {
        this.aggregateFunctionNameFm = aggregateFunctionNameFm;
    }

    public ExpressionStatement getExpressionStatement() {
        return expressionStatement;
    }

    public void setExpressionStatement(ExpressionStatement expressionStatement) {
        this.expressionStatement = expressionStatement;
    }

    public ColumnAliasFragment getColumnAliasFragment() {
        return columnAliasFragment;
    }

    public void setColumnAliasFragment(ColumnAliasFragment columnAliasFragment) {
        this.columnAliasFragment = columnAliasFragment;
    }
}
