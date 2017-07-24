package org.apache.hive.plsql.dml.fragment.updateFragment;

import org.apache.hive.plsql.dml.fragment.selectFragment.IntoClauseFragment;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

import java.util.List;

/**
 * Created by wangsm9 on 2017/7/10.
 * <p>
 * static_returning_clause
 * : (RETURNING | RETURN) expression (',' expression)* into_clause
 * ;
 */
public class StaticReturningClauseFm extends SqlStatement {

    private List<ExpressionStatement> expressionStatements;
    private IntoClauseFragment intoClauseFragment;

    public void addExpression(ExpressionStatement es){
        expressionStatements.add(es);
    }

    public IntoClauseFragment getIntoClauseFragment() {
        return intoClauseFragment;
    }

    public void setIntoClauseFragment(IntoClauseFragment intoClauseFragment) {
        this.intoClauseFragment = intoClauseFragment;
    }
}
