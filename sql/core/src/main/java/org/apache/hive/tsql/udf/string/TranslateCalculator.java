package org.apache.hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

/**
 * Created by dengrb1 on 2/16 0016.
 */
public class TranslateCalculator extends BaseCalculator {
    public TranslateCalculator() {
    }

    /**
     * translate behavior stick to sql-server
     */
    @Override
    public Var compute() throws Exception {
        Var var = new Var("translate func", null, Var.DataType.NULL);
        if (getAllArguments().size() != 3)
            throw new FunctionArgumentException("translate", getAllArguments().size(), 3, 3);
        Var arg1 = getArguments(0);
        Var arg2 = getArguments(1);
        Var arg3 = getArguments(2);
        String inputStr = StrUtils.trimQuot(arg1.getVarValue().toString());
        String fromStr = StrUtils.trimQuot(arg2.getVarValue().toString());
        String toStr = StrUtils.trimQuot(arg3.getVarValue().toString());
        if (fromStr.length() != toStr.length())
            throw new Exception("translate FROM-arg length not equal TO-arg length");
        for (int i = 0; i < fromStr.length(); i++) {
            inputStr = inputStr.replace(fromStr.charAt(i), toStr.charAt(i));
        }
        var.setDataType(Var.DataType.STRING);
        var.setVarValue(inputStr);
        return var;
    }
}
