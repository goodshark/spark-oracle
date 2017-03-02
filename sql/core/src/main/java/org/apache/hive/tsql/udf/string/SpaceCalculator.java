package hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

/**
 * Created by dengrb1 on 2/15 0015.
 */
public class SpaceCalculator extends BaseCalculator {
    public SpaceCalculator() {
    }

    @Override
    public Var compute() throws Exception {
        Var var = new Var("space func", null, Var.DataType.NULL);
        if (getAllArguments().size() != 1)
            throw new FunctionArgumentException("space", getAllArguments().size(), 1, 1);
        Var arg = getArguments(0);
        String argStr = StrUtils.trimQuot(arg.getVarValue().toString());
        int cnt = Integer.parseInt(argStr);
        if (cnt < 0)
            return var;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < cnt; i++) {
            sb.append(" ");
        }
        String resStr = sb.toString();
        var.setDataType(Var.DataType.STRING);
        var.setVarValue(resStr);
        return var;
    }
}
