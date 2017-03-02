package org.apache.hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

/**
 * Created by dengrb1 on 2/16 0016.
 */
public class StringSplitCalculator extends BaseCalculator {
    public StringSplitCalculator() {
    }

    @Override
    public Var compute() throws Exception {
        Var var = new Var("string_split func", null, Var.DataType.NULL);
        if (getAllArguments().size() != 2)
            throw new FunctionArgumentException("string_split", getAllArguments().size(), 2, 2);
        Var arg1 = getArguments(0);
        Var arg2 = getArguments(1);
        String argStr = StrUtils.trimQuot(arg1.getVarValue().toString());
        String sepStr = StrUtils.trimQuot(arg2.getVarValue().toString());
        String[] strs;
        if (!sepStr.isEmpty()) {
            strs = argStr.split(sepStr.substring(0, 1));
        } else {
            strs = argStr.split(sepStr);
        }
        // TODO Var do NOT support LIST type
        var.setVarValue(strs);
        return var;
    }
}
