package org.apache.hive.plsql.dml.fragment.updateFragment;

import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.commonFragment.IdFragment;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/10.
 * : SET
 * (column_based_update_set_clause (',' column_based_update_set_clause)* | VALUE '(' id ')' '=' expression)
 * ;
 */
public class UpdateSetClauseFm extends SqlStatement {
    private List<ColumnBasedUpDateFm> columnBasedUpDateFms = new ArrayList<>();
    private String value;
    private IdFragment idFragment;
    private ExpressionStatement expressionStatement;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(" SET ");
        sql.append(FragMentUtils.appendOriginalSql(columnBasedUpDateFms, getExecSession()));

        //TODO 暂时没有发现这样的语法 VALUE '(' id ')' '=' expression)

        return sql.toString();
    }

    public void addColumnBaseUpdate(ColumnBasedUpDateFm cbudf) {
        columnBasedUpDateFms.add(cbudf);
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public IdFragment getIdFragment() {
        return idFragment;
    }

    public void setIdFragment(IdFragment idFragment) {
        this.idFragment = idFragment;
    }

    public ExpressionStatement getExpressionStatement() {
        return expressionStatement;
    }

    public void setExpressionStatement(ExpressionStatement expressionStatement) {
        this.expressionStatement = expressionStatement;
    }
}
