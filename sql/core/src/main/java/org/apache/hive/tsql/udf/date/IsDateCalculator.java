package org.apache.hive.tsql.udf.date;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.func.DateUnit;
import org.apache.hive.tsql.udf.BaseCalculator;
import org.apache.hive.tsql.util.StrUtils;

import java.text.ParseException;
import java.util.Date;

/**
 * Created by zhongdg1 on 2017/2/8.
 */
public class IsDateCalculator extends BaseCalculator {
    public IsDateCalculator() {
        setMinMax(1);
    }

    @Override
    public Var compute() throws Exception {
        int result = 1;
        try {
            getArguments(0).getDate();
        } catch (ParseException e) {
            result = 0;
        }

        return new Var(result, Var.DataType.INT);
    }


}
