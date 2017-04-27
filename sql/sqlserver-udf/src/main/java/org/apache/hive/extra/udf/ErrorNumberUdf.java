package org.apache.hive.extra.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by wangsm9 on 2017/4/27.
 */
public class ErrorNumberUdf extends UDF {
    public String evaluate() {
        return "0";
    }
}
