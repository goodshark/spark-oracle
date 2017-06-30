package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.hive.plsql.dml.fragment.FragMentUtils;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.List;

/**
 * Created by wangsm9 on 2017/6/21.
 */
public class SubqueryFactoringClause extends SqlStatement {
    private List<SqlStatement> factoringElements;

    public List<SqlStatement> getFactoringElements() {
        return factoringElements;
    }

    public void setFactoringElements(List<SqlStatement> factoringElements) {
        this.factoringElements = factoringElements;
    }

    @Override
    public String getFinalSql() throws Exception {
        StringBuffer sql = new StringBuffer();
        sql.append(Common.SPACE).append("with ");
        sql.append(FragMentUtils.appendFinalSql(factoringElements));
        sql.append(Common.SPACE);
        return sql.toString();
    }

    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(Common.SPACE).append("with ");
        sql.append(FragMentUtils.appendOriginalSql(factoringElements));
        sql.append(Common.SPACE);
        return sql.toString();
    }
}
