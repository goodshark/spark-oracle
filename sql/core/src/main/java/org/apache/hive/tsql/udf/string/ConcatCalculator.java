package hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

import java.util.List;

/**
 * Created by dengrb1 on 2/15 0015.
 */
public class ConcatCalculator extends BaseCalculator {
    public ConcatCalculator() {
    }

    @Override
    public Var compute() throws Exception {
        Var var = new Var("concat func", null, Var.DataType.NULL);
        List<Var> argList = getAllArguments();
        if (argList.size() < 2)
            throw new FunctionArgumentException("concat", argList.size(), 2, Integer.MAX_VALUE);

        StringBuilder sb = new StringBuilder();
        for (Var arg : argList) {
            if (arg == null || arg.getVarValue() == null || arg.getDataType() == Var.DataType.NULL)
                continue;
            String argStr = StrUtils.trimQuot(arg.getVarValue().toString());
            sb.append(argStr);
        }

        var.setDataType(Var.DataType.STRING);
        var.setVarValue(sb.toString());
        return var;
    }
}
