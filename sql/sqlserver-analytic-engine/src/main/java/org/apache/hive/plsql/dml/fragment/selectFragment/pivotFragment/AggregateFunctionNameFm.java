package org.apache.hive.plsql.dml.fragment.selectFragment.pivotFragment;

import org.apache.hive.plsql.dml.commonFragment.IdFragment;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/6.
 * aggregate_function_name
 * : id ('.' id_expression)*
 * ;
 */
public class AggregateFunctionNameFm extends SqlStatement {
    private IdFragment idFragment;
    private List<ExpressionStatement> idExpressions = new ArrayList<>();

    public IdFragment getIdFragment() {
        return idFragment;
    }

    public void setIdFragment(IdFragment idFragment) {
        this.idFragment = idFragment;
    }

    public void addExpression(ExpressionStatement idExpression) {
        idExpressions.add(idExpression);
    }
}
