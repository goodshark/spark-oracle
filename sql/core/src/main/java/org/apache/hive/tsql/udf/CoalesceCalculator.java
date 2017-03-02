package org.apache.hive.tsql.udf;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.exception.WrongArgNumberException;

/**
 * Created by zhongdg1 on 2017/2/6.
 */
public class CoalesceCalculator extends BaseCalculator {
    public CoalesceCalculator() {
        setMinSize(1);
        setMaxSize(Integer.MAX_VALUE);
    }

    @Override
    public Var compute() throws Exception {
        Var result = null;
        for (Var var : getArguments()) {
            if (null == var || null == var.getVarValue() || var.getDataType() == Var.DataType.NULL) {
                continue;
            }
            result = var;
            break;
        }
        if(null == result) {
            throw new WrongArgNumberException("Function coalesce must include non-NULL argument.");
        }
        return result;
    }
}
