package org.apache.hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

/**
 * Created by dengrb1 on 2/16 0016.
 */
public class StuffCalculator extends BaseCalculator {
    public StuffCalculator() {
    }

    @Override
    public Var compute() throws Exception {
        Var var = new Var("stuff func", null, Var.DataType.NULL);
        if (getAllArguments().size() != 4)
            throw new FunctionArgumentException("stuff", getAllArguments().size(), 4, 4);

        Var arg1 = getArguments(0);
        Var arg2 = getArguments(1);
        Var arg3 = getArguments(2);
        Var arg4 = getArguments(3);
        String targetStr = StrUtils.trimQuot(arg1.getVarValue().toString());
        int startIn = Integer.parseInt(StrUtils.trimQuot(arg2.getVarValue().toString()));
        int endIn = Integer.parseInt(StrUtils.trimQuot(arg3.getVarValue().toString()));
        String repStr = StrUtils.trimQuot(arg4.getVarValue().toString());
        // index stick to sql-server, start from 1
        if (startIn <= 0 || endIn < 0 || startIn > targetStr.length())
            return var;
        StringBuilder sb = new StringBuilder();
        sb.append(targetStr.substring(0, startIn-1));
        sb.append(repStr);
        sb.append(targetStr.substring(startIn+endIn-1));

        var.setDataType(Var.DataType.STRING);
        var.setVarValue(sb.toString());
        return var;
    }
}
