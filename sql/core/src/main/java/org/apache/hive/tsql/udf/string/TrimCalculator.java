package org.apache.hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

/**
 * Created by dengrb1 on 2/15 0015.
 */
public class TrimCalculator extends BaseCalculator {
    public TrimCalculator() {
        setMinSize(1);
        setCheckNull(false);
    }

    @Override
    public Var compute() throws Exception {
        Var var = new Var("trim func", null, Var.DataType.NULL);
        String resStr = "";

        int argNum = getSize();
        if (argNum == 1) {
            Var arg = getArguments(0);
            String argStr = StrUtils.trimQuot(arg.getVarValue() == null ? "" : arg.getVarValue().toString());
            resStr = argStr.trim();
        } else {
            Var arg1 = getArguments(0);
            Var arg2 = getArguments(1);
            String repStr = StrUtils.trimQuot(arg1.getVarValue() == null ? "" : arg1.getVarValue().toString());
            String targetStr = StrUtils.trimQuot(arg2.getVarValue() == null ? "" : arg2.getVarValue().toString());
            for (Character ch: repStr.toCharArray()) {
                targetStr = targetStr.replace(ch.toString(), "");
            }
            resStr = targetStr;
        }

        var.setDataType(Var.DataType.STRING);
        var.setVarValue(resStr);
        return var;
    }
}
