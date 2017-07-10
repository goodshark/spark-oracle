package org.apache.hive.plsql.dml.fragment.selectFragment.pivotFragment;

import org.apache.hive.plsql.dml.commonFragment.ExpressionListFragment;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

/**
 * Created by wangsm9 on 2017/7/6.
 * <p>
 * * pivot_in_clause_elements
 * : expression
 * | expression_list
 * ;
 */
public class PivotInClauseElemetsFm extends SqlStatement {

    private ExpressionStatement expressionStatement;
    private ExpressionListFragment expressionListFragments;

    public ExpressionStatement getExpressionStatement() {
        return expressionStatement;
    }

    public void setExpressionStatement(ExpressionStatement expressionStatement) {
        this.expressionStatement = expressionStatement;
    }

    public ExpressionListFragment getExpressionListFragments() {
        return expressionListFragments;
    }

    public void setExpressionListFragments(ExpressionListFragment expressionListFragments) {
        this.expressionListFragments = expressionListFragments;
    }
}
