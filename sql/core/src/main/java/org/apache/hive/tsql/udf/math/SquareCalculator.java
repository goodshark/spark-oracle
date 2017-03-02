package hive.tsql.udf.math;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.udf.BaseCalculator;

/**
 * Created by zhongdg1 on 2017/1/19.
 */
public class SquareCalculator extends BaseCalculator {
    public SquareCalculator() {
        setMinMax(1);
    }

    @Override
    public Var compute() throws Exception {
        Float val = getArguments(0).getFloat();
        return new Var(val*val, Var.DataType.FLOAT);
    }
}
