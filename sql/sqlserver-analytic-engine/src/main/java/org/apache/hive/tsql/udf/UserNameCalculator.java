package org.apache.hive.tsql.udf;

import org.apache.hive.tsql.arg.Var;

/**
 * Created by wangsm9 on 2017/4/26.
 */
public class UserNameCalculator extends BaseCalculator {
    @Override
    public Var compute() throws Exception {
        String userName = getExecSession().getSparkSession().sparkSessionUserName();
        return new Var(userName, Var.DataType.STRING);
    }
}
