package org.apache.hive.tsql.udf.string;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.udf.BaseCalculator;

/**
 * Created by zhongdg1 on 2017/1/19.
 */
public class SubStringCalculator extends BaseCalculator {

    public SubStringCalculator() {
        setMinMax(3);
    }

    @Override
    public Var compute() {
        String source = getArguments(0).getString();
        int beginIndex = getArguments(1).getInt();
        int length = getArguments(2).getInt();
        if (length < 0) {
            throw new IllegalArgumentException("Substring function argument length cannot less zero.");
        }
        if (beginIndex < 0 || length == 0) {
            return new Var("", Var.DataType.STRING);
        }

        int endIndex = beginIndex + length;
        endIndex = endIndex < source.length() ? endIndex : source.length();
        if (beginIndex < 0) {
            beginIndex = 0;
        }
        if (beginIndex > source.length()) {
            beginIndex = source.length();
        }
        return new Var(source.substring(beginIndex, endIndex), Var.DataType.STRING);
    }
}
