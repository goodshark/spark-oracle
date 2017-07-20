package org.apache.hive.plsql.dml.commonFragment;

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

/**
 * Created by wangsm9 on 2017/7/4.
 * <p>
 * * tableview_name
 * : id ('.' id_expression)?
 * ('@' link_name | /* partition_extension_clause)?
 */
public class TableViewNameFragment extends SqlStatement {

    private IdFragment idFragment;
    private ExpressionStatement idExpression;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(FragMentUtils.appendOriginalSql(idFragment, getExecSession()));

        if (null != idExpression) {
            sql.append(".");
            sql.append(FragMentUtils.appendOriginalSql(idExpression, getExecSession()));
        }
        return sql.toString();
    }

    public IdFragment getIdFragment() {
        return idFragment;
    }

    public void setIdFragment(IdFragment idFragment) {
        this.idFragment = idFragment;
    }

    public ExpressionStatement getIdExpression() {
        return idExpression;
    }

    public void setIdExpression(ExpressionStatement idExpression) {
        this.idExpression = idExpression;
    }
}
