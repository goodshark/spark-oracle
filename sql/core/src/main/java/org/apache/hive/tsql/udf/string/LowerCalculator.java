package hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

/**
 * Created by dengrb1 on 2/15 0015.
 */
public class LowerCalculator extends BaseCalculator {
    public LowerCalculator() {
    }

    @Override
    public Var compute() throws Exception {
        Var var = new Var("lower func", null, Var.DataType.NULL);
        if (getAllArguments().size() != 1)
            throw new FunctionArgumentException("lower", getAllArguments().size(), 1, 1);
        Var arg = getArguments(0);
        String argStr = StrUtils.trimQuot(arg.getVarValue().toString());
        String resStr = argStr.toLowerCase();
        var.setDataType(Var.DataType.STRING);
        var.setVarValue(resStr);
        return var;
    }
}
