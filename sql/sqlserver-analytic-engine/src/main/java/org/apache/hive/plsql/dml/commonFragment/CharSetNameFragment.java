package org.apache.hive.plsql.dml.commonFragment;

import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/3.
 * char_set_name
 * : id_expression ('.' id_expression)*
 * ;
 */
public class CharSetNameFragment extends SqlStatement {


    private List<String> idExpressions = new ArrayList<>();


    public void addIdExprssions(String idExpression) {
        idExpressions.add(idExpression);
    }

    @Override
    public String getOriginalSql() {
        return FragMentUtils.appendSql(idExpressions);
    }

    @Override
    public String getFinalSql() throws Exception {
        return getOriginalSql();
    }
}
