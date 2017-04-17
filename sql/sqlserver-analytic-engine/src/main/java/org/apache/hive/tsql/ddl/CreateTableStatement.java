package org.apache.hive.tsql.ddl;

import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.common.TmpTableNameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Set;

/**
 * Created by wangsm9 on 2017/1/4.
 */
public class CreateTableStatement extends SqlStatement {

    private String tableName;
    private String columnDefs;
    private String crudStr = "";
    private static final Logger LOG = LoggerFactory.getLogger(CreateTableStatement.class);

    @Override
    public int execute() {
        StringBuffer sb = new StringBuffer();
        sb.append("CREATE TABLE ");
        TmpTableNameUtils tableNameUtils = new TmpTableNameUtils();
        String realTableName = tableNameUtils.createTableName(tableName);
        sb.append(realTableName)
                .append(" (")
                .append(columnDefs)
                .append(")")
                .append(Common.SPACE)
                .append(crudStr);
        /**
         *  key=1 表示存储的表变量 DECLARE @t_a as TABLE(name VARCHAR(50))
         * key=2 表示存储的临时表 #t1
         * key=3 表示存储的全局表 ##t2
         */

        if (tableNameUtils.checkIsTmpTable(tableName)) {
            addTmpTable(tableName, realTableName);
            getExecSession().getSparkSession().addTableToSparkSeesion(tableName,realTableName,2);
        }
        if(tableNameUtils.checkIsGlobalTmpTable(tableName)){
            addTmpTable(tableName, realTableName);
            getExecSession().getSparkSession().addTableToSparkSeesion(tableName,realTableName,3);
        }
        setAddResult(false);
        commitStatement(sb.toString());
        return 1;
    }

    /*private void addTableToSparkSeesion(String tableName,String tableAliasName, HashMap<Integer, HashMap<String,String>> map , int key) {
        if(null!=map.get(key)){
            map.get(key).put(tableName,tableAliasName);
        }else{
            HashMap<String,String> tb = new HashMap<>();
            tb.put(tableName,tableAliasName);
            map.put(key,tb);
        }
    }*/

    public CreateTableStatement(String tableName) {
        this.tableName = tableName;
    }

    public void setColumnDefs(String columnDefs) {
        this.columnDefs = columnDefs;
    }

    public void setCrudStr(String crudStr) {
        this.crudStr = crudStr;
    }


}
