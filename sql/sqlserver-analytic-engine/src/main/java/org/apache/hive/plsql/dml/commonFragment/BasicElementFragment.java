package org.apache.hive.plsql.dml.commonFragment;

import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by dengrb1 on 6/9 0009.
 * subquery_basic_elements
 * : query_block
 * | '(' subquery ')'
 * ;
 */
public class BasicElementFragment extends SqlStatement {
    private SqlStatement queryBlock;
    private SqlStatement subQuery;


    public SqlStatement getQueryBlock() {
        return queryBlock;
    }

    public void setQueryBlock(SqlStatement queryBlock) {
        this.queryBlock = queryBlock;
    }

    public SqlStatement getSubQuery() {
        return subQuery;
    }

    public void setSubQuery(SqlStatement subQuery) {
        this.subQuery = subQuery;
    }

    @Override
    public String getSql() {
        return "";
    }

    @Override
    public String getOriginalSql() {
        StringBuffer stringBuffer = new StringBuffer();
        if (null != queryBlock) {
            stringBuffer.append(FragMentUtils.appendOriginalSql(queryBlock, getExecSession()));
        }
        if (null != subQuery) {
            stringBuffer.append("(");
            stringBuffer.append(FragMentUtils.appendOriginalSql(subQuery, getExecSession()));
            stringBuffer.append(")");
        }
        return stringBuffer.toString();
    }

}
