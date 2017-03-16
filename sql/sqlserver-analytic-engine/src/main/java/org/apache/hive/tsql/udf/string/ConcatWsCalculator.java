package org.apache.hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

/**
 * Created by dengrb1 on 2/16 0016.
 */
public class ConcatWsCalculator extends BaseCalculator {
    public ConcatWsCalculator() {
        setCheckNull(false);
    }

    @Override
    public Var compute() throws Exception {
        Var var = new Var("concat_ws func", null, Var.DataType.NULL);
        if (getAllArguments().size() < 3)
            throw new FunctionArgumentException("concat_ws", getAllArguments().size(), 3, Integer.MAX_VALUE);
        Var arg1 = getArguments(0);
        String concatStr = StrUtils.trimQuot(arg1.getVarValue().toString());
        StringBuilder sb = new StringBuilder();
        for (Var arg: getAllArguments().subList(1, getAllArguments().size())) {
            if (arg == null || arg.getVarValue() == null || arg.getDataType() == Var.DataType.NULL)
                continue;
            String argStr = StrUtils.trimQuot(arg.getVarValue().toString());
            if (sb.length() == 0) {
                sb.append(argStr);
            } else {
                sb.append(concatStr).append(argStr);
            }
        }
        var.setDataType(Var.DataType.STRING);
        var.setVarValue(sb.toString());
        return var;
    }
}
