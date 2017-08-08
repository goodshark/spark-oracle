package org.apache.hive.plsql.dml.fragment.mergeFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnNameFragment;
import org.apache.hive.plsql.dml.commonFragment.ExpressionListFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.WhereClauseFragment;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/8/4.
 * merge_insert_clause
 * : WHEN NOT MATCHED THEN INSERT ('(' column_name (',' column_name)* ')')? VALUES expression_list where_clause?
 * ;
 */
public class MergerInsertClauseFm extends SqlStatement {


    private List<ColumnNameFragment> columnNames = new ArrayList<>();
    private ExpressionListFragment expressionListFragment;
    private WhereClauseFragment whereClauseFragment;

    public void addColumnName(ColumnNameFragment columnNameFragment) {
        columnNames.add(columnNameFragment);
    }

    public ExpressionListFragment getExpressionListFragment() {
        return expressionListFragment;
    }

    public void setExpressionListFragment(ExpressionListFragment expressionListFragment) {
        this.expressionListFragment = expressionListFragment;
    }

    public WhereClauseFragment getWhereClauseFragment() {
        return whereClauseFragment;
    }

    public void setWhereClauseFragment(WhereClauseFragment whereClauseFragment) {
        this.whereClauseFragment = whereClauseFragment;
    }

    public List<ColumnNameFragment> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<ColumnNameFragment> columnNames) {
        this.columnNames = columnNames;
    }
}
