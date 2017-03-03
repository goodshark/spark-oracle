package org.apache.hive.tsql.udf.string;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.udf.BaseCalculator;

/**
 * Created by zhongdg1 on 2017/1/19.
 */
public class LenCalculator extends BaseCalculator {
    public LenCalculator() {
        setMinMax(1);
    }

    @Override
    public Var compute() {
        return new Var(trimRight(getArguments(0).getString()).length(), Var.DataType.INT);
    }

    public String trimRight(String str) {
        if (StringUtils.isBlank(str)) {
            return "";
        }
        int index = 0;
        for (int i = str.length() - 1; i >= 0; i--) {
            char c = str.charAt(i);
            if (c != ' ') {
                index = i;
                break;
            }
        }
        return str.substring(0, index + 1);
    }
}
