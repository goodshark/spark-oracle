package org.apache.hive.plsql.dml.fragment.insertFragment;

import org.apache.hive.plsql.dml.commonFragment.ExpressionListFragment;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/11.
 * values_clause
 * : VALUES expression_list
 */
public class ValuesClauseFragment extends SqlStatement {


    private ExpressionListFragment expressionListFragment;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(FragMentUtils.appendOriginalSql(expressionListFragment, getExecSession()));
        return sql.toString();
    }

    public ExpressionListFragment getExpressionListFragment() {
        return expressionListFragment;
    }

    public void setExpressionListFragment(ExpressionListFragment expressionListFragment) {
        this.expressionListFragment = expressionListFragment;
    }
}
