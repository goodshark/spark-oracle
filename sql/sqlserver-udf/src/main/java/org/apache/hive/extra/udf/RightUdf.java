package org.apache.hive.extra.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Created by zhongdg1 on 2017/4/10.
 */
public class RightUdf extends UDF {

    public String evaluate(String str, int length) {
        if (StringUtils.isBlank(str) || length < 1) {
            return "";
        }
        int sourceLen = str.length();
        int startIndex = sourceLen - length;
        if(startIndex < 0) {
            startIndex = 0;
        }
        return str.substring(startIndex);
    }

}
