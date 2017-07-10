package org.apache.hive.plsql.dml.commonFragment;

import org.apache.hive.plsql.dml.commonFragment.IdFragment;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/4.
 *
 * * tableview_name
 * : id ('.' id_expression)?
 * ('@' link_name | /* partition_extension_clause)?
 */
public class TableViewNameFragment extends SqlStatement {

    private IdFragment idFragment;
    private String idExpression;

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
