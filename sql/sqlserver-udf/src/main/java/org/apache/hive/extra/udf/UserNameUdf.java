package org.apache.hive.extra.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hive.tsql.udf.UserNameCalculator;

/**
 * Created by wangsm9 on 2017/4/27.
 */
public class UserNameUdf extends UDF {
    private UserNameCalculator userNameCalculator = new UserNameCalculator();

    public String evaluate() {
        String un = "";
        try {
            un = userNameCalculator.compute().getVarValue().toString();
        } catch (Exception e) {
            System.out.println(" get userName by udf fuction error ."+e);
        }
        return un;
    }
}
