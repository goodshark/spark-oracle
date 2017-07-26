package org.apache.hive.plsql.dml.fragment.updateFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnNameFragment;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.fragment.selectFragment.SubqueryFragment;
import org.apache.hive.tsql.ExecSession;
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
    private ExpressionStatement expressionStatement;
    private SubqueryFragment subqueryFragment;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        if (null != subqueryFragment) {
            //TODO
        }
        if (null != expressionStatement) {
            sql.append(FragMentUtils.appendOriginalSql(columnNameFragments.get(0), getExecSession()));
            sql.append("=");
            try {
                sql.append(FragMentUtils.appendFinalSql(expressionStatement, getExecSession()));
            }catch (Exception e){

            }

        }
        return sql.toString();
    }

    public void addColumnFm(ColumnNameFragment cnf) {
        columnNameFragments.add(cnf);
    }


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
