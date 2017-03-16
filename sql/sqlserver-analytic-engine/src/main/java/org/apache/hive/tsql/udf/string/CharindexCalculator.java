package org.apache.hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

/**
 * Created by dengrb1 on 2/16 0016.
 */
public class CharindexCalculator extends BaseCalculator {
    public CharindexCalculator() {
    }

    /**
     * pos from 1 ... n
     * @return Var(INT)
     * @throws Exception
     */
    @Override
    public Var compute() throws Exception {
        Var var = new Var("charindex func", null, Var.DataType.NULL);
        if (getAllArguments().size() < 2)
            throw new FunctionArgumentException("charindex", getAllArguments().size(), 2, 3);

        Var arg1 = getArguments(0);
        Var arg2 = getArguments(1);
        String findStr = StrUtils.trimQuot(arg1.getVarValue().toString());
        String searchStr = StrUtils.trimQuot(arg2.getVarValue().toString());
        int pos = 0;
        if (getAllArguments().size() == 3) {
            Var arg3 = getArguments(2);
            int startIndex = Integer.parseInt(StrUtils.trimQuot(arg3.getVarValue().toString()));
            pos = searchStr.indexOf(findStr, startIndex - 1) + 1;
        } else {
            pos = searchStr.indexOf(findStr) + 1;
        }
        var.setDataType(Var.DataType.INT);
        var.setVarValue(pos);
        return var;
    }
}
