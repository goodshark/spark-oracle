package hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

/**
 * Created by dengrb1 on 2/17 0017.
 */
public class LeftCalculator extends BaseCalculator {
    public LeftCalculator() {
    }

    @Override
    public Var compute() throws Exception {
        Var var = new Var("left func", null, Var.DataType.NULL);
        if (getAllArguments().size() != 2)
            throw new FunctionArgumentException("left", getAllArguments().size(), 2, 2);

        Var arg1 = getArguments(0);
        Var arg2 = getArguments(1);
        String targetStr = StrUtils.trimQuot(arg1.getVarValue().toString());
        int n = Integer.parseInt(StrUtils.trimQuot(arg2.getVarValue().toString()));
        if (n < 0) {
            throw new Exception("The second arg of LEFT function require positive number");
        } else if (n > targetStr.length()) {
            n = targetStr.length();
        }
        var.setDataType(Var.DataType.STRING);
        var.setVarValue(targetStr.substring(0, n));
        return var;
    }
}
