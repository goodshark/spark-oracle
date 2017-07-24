package org.apache.hive.plsql.dml.fragment.selectFragment.joinFragment;

import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

/**
 * Created by wangsm9 on 2017/7/5.
 * ; * join_on_part
 * : ON condition
 */
public class JoinOnPartFragment extends SqlStatement {
    private ExpressionStatement es;

    @Override
    public String getOriginalSql() {
        return "ON " + FragMentUtils.appendOriginalSql(es, getExecSession());
    }

    public ExpressionStatement getEs() {
        return es;
    }

    public void setEs(ExpressionStatement es) {
        this.es = es;
    }
}
