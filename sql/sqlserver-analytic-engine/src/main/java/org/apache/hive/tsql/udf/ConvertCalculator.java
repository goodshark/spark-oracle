package org.apache.hive.tsql.udf;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.func.DateUnit;

import java.util.Date;

/**
 * Created by zhongdg1 on 2017/2/6.
 */
public class ConvertCalculator extends BaseCalculator {
    public ConvertCalculator() {
        setMinMax(1);
    }

    @Override
    public Var compute() throws Exception {
        Var var = getArguments(0);

        switch (var.getDataType()) {
            case DATE:
                Date date = var.getDate();
                StringBuffer sb = new StringBuffer();
                sb.append(getDatePartValue(DateUnit.YEAR, date)).append("-")
                        .append(getDatePartValue(DateUnit.MONTH, date)).append("-")
                        .append(getDatePartValue(DateUnit.DAY, date));
                var.setVarValue(sb.toString());
                break;
            default:
                break;
        }
        return var;
    }
}
