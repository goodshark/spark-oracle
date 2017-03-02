package hive.tsql.udf.date;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.func.DateUnit;
import org.apache.hive.tsql.udf.BaseCalculator;

import java.util.Date;

/**
 * Created by zhongdg1 on 2017/2/8.
 */
public class DateNameCalculator extends BaseCalculator {
    public DateNameCalculator() {
        setMinMax(2);
    }

    @Override
    public Var compute() throws Exception {
        DateUnit dateUnit = (DateUnit) getArguments(0).getVarValue();
        Date date = getArguments(1).getDate();
        return new Var(getDatePartValue(dateUnit, date), Var.DataType.INT);
    }
}
