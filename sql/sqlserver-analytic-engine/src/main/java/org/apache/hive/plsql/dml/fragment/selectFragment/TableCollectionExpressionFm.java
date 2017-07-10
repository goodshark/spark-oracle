package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

/**
 * Created by wangsm9 on 2017/7/5.
 *
 * table_collection_expression
 : (TABLE | THE) ('(' subquery ')' | '(' expression ')' ('(' '+' ')')?)
 */
public class TableCollectionExpressionFm extends SqlStatement {

    private String keyWord;
    private SubqueryFragment subqueryFragment;
    private ExpressionStatement expressionStatement;

    public String getKeyWord() {
        return keyWord;
    }

    public void setKeyWord(String keyWord) {
        this.keyWord = keyWord;
    }

    public SubqueryFragment getSubqueryFragment() {
        return subqueryFragment;
    }

    public void setSubqueryFragment(SubqueryFragment subqueryFragment) {
        this.subqueryFragment = subqueryFragment;
    }

    public ExpressionStatement getExpressionStatement() {
        return expressionStatement;
    }

    public void setExpressionStatement(ExpressionStatement expressionStatement) {
        this.expressionStatement = expressionStatement;
    }
}
