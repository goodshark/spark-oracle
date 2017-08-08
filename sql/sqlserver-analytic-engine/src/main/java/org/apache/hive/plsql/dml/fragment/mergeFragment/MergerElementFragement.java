package org.apache.hive.plsql.dml.fragment.mergeFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnNameFragment;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

/**
 * Created by wangsm9 on 2017/8/4.
 * merge_element
 * : column_name '=' expression
 * ;
 */
public class MergerElementFragement extends SqlStatement {
    private ColumnNameFragment columnNameFragment;
    private ExpressionStatement expressionStatement;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(FragMentUtils.appendOriginalSql(columnNameFragment, getExecSession()));
        sql.append("=");
        sql.append(FragMentUtils.appendOriginalSql(expressionStatement, getExecSession()));
        return  sql.toString();
    }

    public ColumnNameFragment getColumnNameFragment() {
        return columnNameFragment;
    }

    public void setColumnNameFragment(ColumnNameFragment columnNameFragment) {
        this.columnNameFragment = columnNameFragment;
    }

    public ExpressionStatement getExpressionStatement() {
        return expressionStatement;
    }

    public void setExpressionStatement(ExpressionStatement expressionStatement) {
        this.expressionStatement = expressionStatement;
    }
}
