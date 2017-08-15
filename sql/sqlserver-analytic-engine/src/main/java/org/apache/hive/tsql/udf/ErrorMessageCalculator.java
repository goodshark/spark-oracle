package org.apache.hive.tsql.udf;

import org.apache.hive.tsql.arg.Var;

/**
 * Created by dengrb1 on 4/17 0017.
 */
public class ErrorMessageCalculator extends BaseCalculator {
    public ErrorMessageCalculator() {
        setCheckNull(false);
        setMinSize(0);
    }

    @Override
    public Var compute() throws Exception {
        return new Var(getExecSession().getErrorStr().replace("'", "\"").replace(";", " "), Var.DataType.STRING);
    }
}
