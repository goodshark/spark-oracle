package hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

/**
 * Created by dengrb1 on 2/17 0017.
 */
public class CharCalculator extends BaseCalculator {
    public CharCalculator() {
    }

    @Override
    public Var compute() throws Exception {
        Var var = new Var("char func", null, Var.DataType.NULL);
        if (getAllArguments().size() != 1)
            throw new FunctionArgumentException("char", getAllArguments().size(), 1, 1);

        Var arg = getArguments(0);
        int n = Integer.parseInt(StrUtils.trimQuot(arg.getVarValue().toString()));
        if (n < 0 || n > 255)
            return var;
        char ch = (char) n;
        var.setDataType(Var.DataType.STRING);
        var.setVarValue(Character.toString(ch));
        return var;
    }
}
