package org.apache.hive.plsql.dml.commonFragment;

import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

import java.util.List;

/**
 * Created by wangsm9 on 2017/7/10.
 * expression_list
 * : '(' expression? (',' expression)* ')'
 * ;
 */
public class ExpressionListFragment extends SqlStatement {

    private List<ExpressionStatement> expressionStatements;

    public void addExpression(ExpressionStatement es) {
        expressionStatements.add(es);
    }


    @Override
    public String getOriginalSql() {
        return FragMentUtils.appendOriginalSql(expressionStatements,getExecSession());
    }
}
