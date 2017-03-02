package hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

import java.util.List;

/**
 * Created by dengrb1 on 2/15 0015.
 */
public class ReplaceCalculator extends BaseCalculator {
    public ReplaceCalculator() {
    }

    @Override
    public Var compute() throws Exception {
        Var var = new Var("replace func", null, Var.DataType.NULL);
        List<Var> argList = getAllArguments();
        if (argList.size() != 3)
            throw new FunctionArgumentException("REPLACE", argList.size(), 3, 3);

        Var arg1 = getArguments(0);
        Var arg2 = getArguments(1);
        Var arg3 = getArguments(2);

        if (arg1 == null || arg2 == null || arg3 == null ||
            arg1.getDataType() == Var.DataType.NULL ||
            arg2.getDataType() == Var.DataType.NULL ||
            arg3.getDataType() == Var.DataType.NULL)
            return var;

        String exprStr = StrUtils.trimQuot(arg1.getVarValue().toString());
        String patStr = StrUtils.trimQuot(arg2.getVarValue().toString());
        String repStr = StrUtils.trimQuot(arg3.getVarValue().toString());
        String resStr = exprStr.replace(patStr, repStr);

        var.setDataType(Var.DataType.STRING);
        var.setVarValue(resStr);
        return var;
    }
}
