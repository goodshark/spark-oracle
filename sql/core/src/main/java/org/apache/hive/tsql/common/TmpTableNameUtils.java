package org.apache.hive.tsql.common;

import org.apache.commons.lang.StringUtils;

import java.util.Random;

/**
 * Created by wangsm9 on 2017/2/8.
 */
public class TmpTableNameUtils{

    //局部临时表
    private static final String TMP = "#";
    //全局临时表
    private static final String TMP2 = "##";



    public String getRelTableName(String tableName) {
        StringBuffer sb = new StringBuffer();
        //TODO GET FROM HADOOP CONF
        if (checkIsGlobalTmpTable(tableName)) {
            sb.append("tmp.");
            int index = tableName.indexOf(TMP2);
            sb.append(tableName.substring(index+2, tableName.length()));
            return sb.toString();
        } else if (checkIsTmpTable(tableName)) {
            sb.append("tmp.");
            String tbName = "";
            if (StringUtils.isBlank(tbName)) {
                int index = tableName.lastIndexOf(TMP);
                sb.append(tableName.substring(index+1, tableName.length())).
                        append("_").append(System.currentTimeMillis()).append("_")
                        .append(new Random().nextInt(1000)).toString();
                return sb.toString();
            } else {
                return tbName;
            }
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


}
