package org.apache.hive.tsql.udf;

import org.apache.hive.tsql.arg.Var;

import java.util.List;

/**
 * Created by dengrb1 on 4/18 0018.
 */
public class IifCalculator extends BaseCalculator {
    public IifCalculator() {
        setMinMax(3);
        setCheckNull(false);
    }

    @Override
    public Var compute() throws Exception {
        List<Var> vars = getAllArguments();
        Var boolExpr = vars.get(0);
        Var expr1 = vars.get(1);
        Var expr2 = vars.get(2);
        if (boolExpr.getDataType() != Var.DataType.BOOLEAN)
            throw new Exception("IIF 1st expression must be boolean expression");
        if (Boolean.parseBoolean(boolExpr.getVarValue().toString())) {
            return expr1;
        } else {
            return expr2;
        }
    }

}
