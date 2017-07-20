package org.apache.hive.plsql.dml.commonFragment;

import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/3.
 * char_set_name
 * : id_expression ('.' id_expression)*
 * ;
 */
public class CharSetNameFragment extends SqlStatement {


    private List<ExpressionStatement> idExpressions = new ArrayList<>();


    public void addIdExprssions(ExpressionStatement idExpression) {
        idExpressions.add(idExpression);
    }

    @Override
    public String getOriginalSql() {
        return FragMentUtils.appendOriginalSql(idExpressions,getExecSession());
    }

    @Override
    public String getFinalSql() throws Exception {
        return getOriginalSql();
    }
}
