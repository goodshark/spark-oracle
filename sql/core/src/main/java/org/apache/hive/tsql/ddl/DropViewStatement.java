package org.apache.hive.tsql.ddl;

import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.udf.ObjectIdCalculator;

import java.util.List;

/**
 * Created by wangsm9 on 2017/1/4.
 */
public class DropViewStatement extends SqlStatement {
    protected List<String> tableNames;

    @Override
    public int execute() throws Exception{
        String sql = getSql().toString();
        checkTableIsExist();
        for (String tableName : tableNames) {
            String execSql = sql + Common.SPACE + tableName;
            execSql = replaceTableName(tableName, execSql);
            setAddResult(false);
            commitStatement(execSql);
        }
        return 1;
    }

    public void checkTableIsExist() throws Exception{
        ObjectIdCalculator objectIdCalculator = new ObjectIdCalculator();
        objectIdCalculator.setExecSession(getExecSession());
        for (String tableName : tableNames) {
            boolean b1=objectIdCalculator.databaseFind(getExecSession().getRealTableName(tableName),"V");
            if(!b1){
                throw new Exception("Table or view :"+tableName +" is not exist.");
            }
        }
    }

    public DropViewStatement(String name) {
        super(name);
    }


    public void setTableName(List<String> tableNames) {
        this.tableNames = tableNames;
    }
}
