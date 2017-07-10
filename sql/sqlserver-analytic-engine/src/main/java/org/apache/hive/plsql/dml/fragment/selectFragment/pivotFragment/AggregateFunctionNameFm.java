package org.apache.hive.plsql.dml.fragment.selectFragment.pivotFragment;

import org.apache.hive.plsql.dml.commonFragment.IdFragment;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.List;

/**
 * Created by wangsm9 on 2017/7/6.
 * aggregate_function_name
 * : id ('.' id_expression)*
 * ;
 */
public class AggregateFunctionNameFm extends SqlStatement {
    private IdFragment idFragment;
    private List<String> idExpressions;

    public IdFragment getIdFragment() {
        return idFragment;
    }

    public void setIdFragment(IdFragment idFragment) {
        this.idFragment = idFragment;
    }

    public List<String> getIdExpressions() {
        return idExpressions;
    }

    public void setIdExpressions(List<String> idExpressions) {
        this.idExpressions = idExpressions;
    }
}
