package org.apache.hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

/**
 * Created by dengrb1 on 2/15 0015.
 */
public class AsciiCalculator extends BaseCalculator {
    public AsciiCalculator() {
        setMinMax(1);
        setCheckNull(false);
    }

    @Override
    public Var compute() throws Exception {
        Var var = new Var("ascii func", null, Var.DataType.NULL);
        if (getAllArguments().size() != 1)
            throw new FunctionArgumentException("ascii", getAllArguments().size(), 1, 1);

        Var arg = getArguments(0);
        String argStr = StrUtils.trimQuot(arg.getVarValue() == null ? "" : arg.getVarValue().toString());
        if (argStr.isEmpty())
            return var;
        char ch = argStr.charAt(0);
        int ascNum = (int) ch;
        var.setDataType(Var.DataType.INT);
        var.setVarValue(ascNum);
        return var;
    }
}
