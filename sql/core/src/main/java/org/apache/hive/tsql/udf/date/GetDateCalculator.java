package hive.tsql.udf.date;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.udf.BaseCalculator;

import java.util.Date;

/**
 * Created by zhongdg1 on 2017/2/6.
 */
public class GetDateCalculator extends BaseCalculator {
    public GetDateCalculator() {
        setMinMax(0);
    }

    @Override
    public Var compute() throws Exception {
        return new Var(new Date(), Var.DataType.DATE);
    }
}
