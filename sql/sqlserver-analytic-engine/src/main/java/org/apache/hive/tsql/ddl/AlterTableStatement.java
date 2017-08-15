package org.apache.hive.tsql.ddl;

import org.apache.hive.tsql.common.SqlStatement;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by wangsm9 on 2017/1/4.
 */
public class AlterTableStatement extends SqlStatement {
    private HashSet<String> tableNames = new HashSet<>();

    @Override
    public int execute() throws Exception{
        String sql = getSql().toString();
        for (String tableName : tableNames) {
            sql = replaceTableName(tableName, sql);
            setAddResult(false);
            commitStatement(sql);
        }
        return 1;
    }

    public void addTables(Set<String> tableNameList) {
        this.tableNames.addAll(tableNameList);
    }
}
