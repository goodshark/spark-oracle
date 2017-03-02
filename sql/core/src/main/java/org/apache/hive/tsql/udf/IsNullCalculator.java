package hive.tsql.udf;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;

import java.util.List;

/**
 * Created by dengrb1 on 2/22 0022.
 */
public class IsNullCalculator extends BaseCalculator {
    public IsNullCalculator() {
    }

    @Override
    public Var compute() throws Exception {
        List<Var> argList = getAllArguments();
        if (argList.size() != 2)
            throw new FunctionArgumentException("ISNULL", argList.size(), 2, 2);

        Var arg1 = getArguments(0);
        if (arg1 == null || arg1.getDataType() == Var.DataType.NULL) {
            return getArguments(1);
        } else {
            return getArguments(0);
        }
    }
}
