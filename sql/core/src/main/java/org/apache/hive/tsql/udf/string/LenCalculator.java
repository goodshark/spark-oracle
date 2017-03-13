package org.apache.hive.tsql.udf.string;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

/**
 * Created by zhongdg1 on 2017/1/19.
 */
public class LenCalculator extends BaseCalculator {
    public LenCalculator() {
        setMinMax(1);
    }

    @Override
    public Var compute() {
        return new Var(StrUtils.trimRight(getArguments(0).getString()).length(), Var.DataType.INT);
    }


}
