package org.apache.hive.plsql.dml.fragment.updateFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnNameFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.SubqueryFragment;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

import java.util.ArrayList;


/**
 * Created by wangsm9 on 2017/7/10.
 * column_based_update_set_clause
 * : column_name '=' expression
 * | '(' column_name (',' column_name)* ')' '=' subquery
 * ;
 */
public class ColumnBasedUpDateFm extends SqlStatement {
    private java.util.List<ColumnNameFragment> columnNameFragments = new ArrayList<>();


    public void addColumnFm(ColumnNameFragment cnf) {
        columnNameFragments.add(cnf);
    }

    private ExpressionStatement expressionStatement;
    private SubqueryFragment subqueryFragment;

    public ExpressionStatement getExpressionStatement() {
        return expressionStatement;
    }

    public void setExpressionStatement(ExpressionStatement expressionStatement) {
        this.expressionStatement = expressionStatement;
    }

    public SubqueryFragment getSubqueryFragment() {
        return subqueryFragment;
    }

    public void setSubqueryFragment(SubqueryFragment subqueryFragment) {
        this.subqueryFragment = subqueryFragment;
    }
}
