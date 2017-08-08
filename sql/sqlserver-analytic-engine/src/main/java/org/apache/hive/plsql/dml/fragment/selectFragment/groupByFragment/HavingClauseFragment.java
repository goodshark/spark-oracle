package org.apache.hive.plsql.dml.fragment.selectFragment.groupByFragment;

import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

/**
 * Created by wangsm9 on 2017/7/24.
 * having_clause
 * : HAVING condition
 * ;
 */
public class HavingClauseFragment extends SqlStatement {
    private ExpressionStatement expressionStatement;


    @Override
    public String getOriginalSql() {
        return expressionStatement.getOriginalSql();
    }

    public ExpressionStatement getExpressionStatement() {
        return expressionStatement;
    }

    public void setExpressionStatement(ExpressionStatement expressionStatement) {
        this.expressionStatement = expressionStatement;
    }
}
