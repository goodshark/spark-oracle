package org.apache.hive.tsql.udf.date;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.func.DateUnit;
import org.apache.hive.tsql.udf.BaseCalculator;

/**
 * Created by zhongdg1 on 2017/2/8.
 */
public class MonthCalculator extends BaseCalculator {
    public MonthCalculator() {
        setMinMax(1);
    }

    @Override
    public Var compute() throws Exception {
        Var v = getArguments(0);
        boolean flag = false;
        if (v.getDataType() == Var.DataType.INT && 0 == v.getInt()) {
            flag = true;
        }
        return new Var(flag ? 1 : getDatePartValue(DateUnit.MONTH, v.getDate()), Var.DataType.INT);
    }


}
