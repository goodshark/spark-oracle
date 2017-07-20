package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * subquery_basic_elements
 * : query_block
 * | '(' subquery ')'
 * ;
 * Created by wangsm9 on 2017/7/3.
 */
public class SubQuBaseElemFragment extends SqlStatement {

    private QueryBlockFragment queryBlock;
    private SubqueryFragment subQuery;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        if (null != queryBlock) {
            sql.append(FragMentUtils.appendOriginalSql(queryBlock,getExecSession()));
        }
        if (null != subQuery) {
            sql.append(FragMentUtils.appendOriginalSql(subQuery,getExecSession()));
        }
        return sql.toString();
    }

    public SqlStatement getQueryBlock() {
        return queryBlock;
    }

    public void setQueryBlock(QueryBlockFragment queryBlock) {
        this.queryBlock = queryBlock;
    }

    public SqlStatement getSubQuery() {
        return subQuery;
    }

    public void setSubQuery(SubqueryFragment subQuery) {
        this.subQuery = subQuery;
    }
}
