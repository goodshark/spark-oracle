package org.apache.hive.plsql.dml.commonFragment;

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/4.
 * <p>
 * * tableview_name
 * : id ('.' id_expression)?
 * ('@' link_name | /* partition_extension_clause)?
 */
public class TableViewNameFragment extends SqlStatement {

    private IdFragment idFragment;
    private String idExpression;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(idFragment.getOriginalSql());

        if (StringUtils.isNoneBlank(idExpression)) {
            sql.append(".");
            sql.append(idExpression);
        }
        return sql.toString();
    }

    public IdFragment getIdFragment() {
        return idFragment;
    }

    public void setIdFragment(IdFragment idFragment) {
        this.idFragment = idFragment;
    }

    public String getIdExpression() {
        return idExpression;
    }

    public void setIdExpression(String idExpression) {
        this.idExpression = idExpression;
    }
}
