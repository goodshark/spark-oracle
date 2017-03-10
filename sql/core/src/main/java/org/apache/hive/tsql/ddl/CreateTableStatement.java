package org.apache.hive.tsql.ddl;

import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.common.TmpTableNameUtils;
import java.util.HashMap;

import java.util.HashSet;

/**
 * Created by wangsm9 on 2017/1/4.
 */
public class CreateTableStatement extends SqlStatement {

    private String tableName;
    private String columnDefs;
    private String crudStr = "";


    @Override
    public int execute() {
        StringBuffer sb = new StringBuffer();
        sb.append("CREATE TABLE ");
        TmpTableNameUtils tableNameUtils = new TmpTableNameUtils();
        String tableAliasName = tableNameUtils.createTableName(tableName);
        sb.append(tableAliasName)
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
        HashMap<Integer, HashSet<String>> map = getExecSession().getSparkSession().getSqlServerTable();
        if (tableNameUtils.checkIsTmpTable(tableName)) {
            addTmpTable(tableName, tableAliasName);
            addTableToSparkSeesion(tableAliasName, map,2);

        }
        if(tableNameUtils.checkIsGlobalTmpTable(tableName)){
            addTmpTable(tableName, tableAliasName);
            addTableToSparkSeesion(tableAliasName, map,3);
        }
        setAddResult(false);
        commitStatement(sb.toString());
        return 1;
    }

    private void addTableToSparkSeesion(String tableAliasName, HashMap<Integer, HashSet<String>> map , int key) {
        if(null!=map.get(key)){
            map.get(key).add(tableAliasName);
        }else{
            HashSet<String> tb = new HashSet<String>();
            tb.add(tableAliasName);
            map.put(key,tb);
        }
    }

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
