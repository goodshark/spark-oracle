package org.apache.hive.plsql.expression;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.dml.ExpressionStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 8/4 0004.
 */
public class GeneralExpression extends ExpressionStatement {
    private List<ExpressionStatement> generalExprs = new ArrayList<>();

    public GeneralExpression() {
    }

    public GeneralExpression mergeGeneralExpr(GeneralExpression g) {
        for (ExpressionStatement es: g.generalExprs)
            generalExprs.add(es);
        return this;
    }

    public void addGeneralExpr(ExpressionStatement es) {
        generalExprs.add(es);
    }

    private boolean indexSafe(int i) {
        return true;
    }

    private Var getArrayVar(int i, Var var) throws Exception {
        generalExprs.get(i).setExecSession(getExecSession());
        generalExprs.get(i).execute();
        Var indexVar = (Var) generalExprs.get(i).getRs().getObject(0);
        int index = (int) indexVar.getVarValue();
        var = var.getArrayVar(index);
        return var;
    }

    @Override
    public int execute() throws Exception {
        Var var = null;
        for (int i = 0; i < generalExprs.size(); i++) {
            if (var == null) {
                generalExprs.get(i).setExecSession(getExecSession());
                generalExprs.get(i).execute();
                var = (Var) generalExprs.get(i).getRs().getObject(0);
                if (var.getDataType() == Var.DataType.COMPLEX) {
                    // do nothing
                } else if (var.getDataType() == Var.DataType.ARRAY) {
                    i++;
                    if (!indexSafe(i)) throw new Exception("general element expression array need a index");
                    var = getArrayVar(i, var);
                } else {
                    throw new Exception("general element expression need complex or array type");
                }
            } else {
                String innerVarName = generalExprs.get(i).getExpressionBean().getVar().getVarName();
                var = var.getInnerVar(innerVarName);
                if (var == null)
                    throw new Exception("general element expression need complex or array type");
                if (var.getDataType() == Var.DataType.ARRAY) {
                    i++;
                    if (!indexSafe(i)) throw new Exception("general element expression array need a index");
                    var = getArrayVar(i, var);
                }
            }
        }
        if (var != null)
            setRs(new SparkResultSet().addRow(new Object[] {var}));
        else
            throw new Exception("can not find general element var");
        return 0;
    }
}
