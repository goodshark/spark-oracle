package hive.tsql.ddl;

import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.List;

/**
 * Created by wangsm9 on 2017/1/4.
 */
public class DropTableStatement extends SqlStatement {
    protected List<String> tableNames;

    @Override
    public int execute() {
        String sql = getSql().toString();
        for (String tableName : tableNames) {
            String execSql = sql + Common.SPACE + tableName;
            execSql = replaceTableName(tableName, execSql);
            commitStatement(execSql);
        }
        return 1;
    }

    public DropTableStatement(String name) {
        super(name);
    }


    public void setTableName(List<String> tableNames) {
        this.tableNames = tableNames;
    }
}
