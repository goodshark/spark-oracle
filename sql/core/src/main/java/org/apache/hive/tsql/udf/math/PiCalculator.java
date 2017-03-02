package hive.tsql.udf.math;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.udf.BaseCalculator;

/**
 * Created by zhongdg1 on 2017/1/19.
 */
public class PiCalculator extends BaseCalculator {
    public PiCalculator() {
        setMinMax(0);
    }

    @Override
    public Var compute() throws Exception {
        return new Var(Math.PI, Var.DataType.DOUBLE);
    }
}
