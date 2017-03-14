package org.apache.hive.tsql.common;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Random;

/**
 * Created by wangsm9 on 2017/2/8.
 */
public class TmpTableNameUtils {

    //局部临时表
    private static final String TMP = "#";
    //全局临时表
    private static final String TMP2 = "##";

    private static final String STR = "@";


    public String getTableName(String tableName) {
        StringBuffer sb = new StringBuffer();
        //TODO GET FROM HADOOP CONF
        if (checkIsGlobalTmpTable(tableName)) {
            sb.append("tmp.");
            int index = tableName.indexOf(TMP2);
            sb.append("global_");
            sb.append(tableName.substring(index + 2, tableName.length()));
            return sb.toString();
        } else if (checkIsTmpTable(tableName)) {
            sb.append("tmp.");
            int index = tableName.lastIndexOf(TMP);
            sb.append("tmp_");
            sb.append(tableName.substring(index + 1, tableName.length())).append("_");
            sb.append(productTableName());
            return sb.toString();
        } else if (tableName.indexOf(STR) != -1) {
            sb.append("tmp.");
            int index = tableName.lastIndexOf(STR);
            sb.append("var_");
            sb.append(tableName.substring(index + 1)).append("_");
            sb.append(productTableName());
            return sb.toString();
        } else {
            return tableName;
        }
    }

    public boolean checkIsTmpTable(String tbName) {
        if (!checkIsGlobalTmpTable(tbName) && tbName.indexOf(TMP) != -1) {
            return true;
        }
        return false;
    }

    public boolean checkIsGlobalTmpTable(String tbName) {
        if (tbName.indexOf(TMP2) != -1) {
            return true;
        }
        return false;
    }

    private String productTableName() {
        StringBuffer sb = new StringBuffer();
        sb.append(System.currentTimeMillis()).append("_").append(new Random().nextInt(1000));
        return sb.toString();
    }


}
