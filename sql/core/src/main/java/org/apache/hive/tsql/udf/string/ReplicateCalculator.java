package org.apache.hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

/**
 * Created by dengrb1 on 2/15 0015.
 */
public class ReplicateCalculator extends BaseCalculator {
    public ReplicateCalculator() {
    }

    @Override
    public Var compute() throws Exception {
        Var var = new Var("replicate func", null, Var.DataType.NULL);
        if (getAllArguments().size() != 2)
            throw new FunctionArgumentException("replicate", getAllArguments().size(), 2, 2);

        Var arg1 = getArguments(0);
        Var arg2 = getArguments(1);
        String argStr = StrUtils.trimQuot(arg1.getVarValue().toString());
        int cnt = Integer.parseInt(StrUtils.trimQuot(arg2.getVarValue().toString()));
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cnt; i++) {
            sb.append(argStr);
        }
        String resStr = sb.toString();

        var.setDataType(Var.DataType.STRING);
        var.setVarValue(resStr);
        return var;
    }
}
