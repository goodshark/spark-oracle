package hive.tsql.udf.date;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.func.DateUnit;
import org.apache.hive.tsql.udf.BaseCalculator;

import java.util.Date;

/**
 * Created by zhongdg1 on 2017/2/8.
 */
public class HourCalculator extends BaseCalculator {
    public HourCalculator() {
        setMinMax(1);
    }

    @Override
    public Var compute() throws Exception {
        Date date = getArguments(0).getDate();
        return new Var(getDatePartValue(DateUnit.HOUR, date), Var.DataType.INT);
    }


}
