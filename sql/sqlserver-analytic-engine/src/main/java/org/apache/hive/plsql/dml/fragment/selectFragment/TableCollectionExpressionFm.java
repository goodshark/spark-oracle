package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

/**
 * Created by wangsm9 on 2017/7/5.
 * <p>
 * table_collection_expression
 * : (TABLE | THE) ('(' subquery ')' | '(' expression ')' ('(' '+' ')')?)
 */
public class TableCollectionExpressionFm extends SqlStatement {

    private String keyWord;
    private SubqueryFragment subqueryFragment;
    private ExpressionStatement expressionStatement;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(keyWord);
        if (null != subqueryFragment) {
            sql.append("(");
            sql.append(FragMentUtils.appendOriginalSql(subqueryFragment,getExecSession()));
            sql.append(")");
        }
        if (null != expressionStatement) {
            sql.append("(");
            sql.append(FragMentUtils.appendOriginalSql(expressionStatement,getExecSession()));
            sql.append(")");
        }
        return sql.toString();
    }

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
