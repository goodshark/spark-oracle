package hive.tsql.udf;

import org.apache.hive.tsql.arg.Var;

/**
 * Created by zhongdg1 on 2017/2/8.
 */
public class NullIfCalculator extends BaseCalculator {
    public NullIfCalculator() {
        setMinMax(2);
    }

    @Override
    public Var compute() throws Exception {
        Var left = getArguments(0);
        Var right = getArguments(1);

        return left.equals(right) ? null : left;
    }
}
