package hive.tsql.ddl;

import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.common.TmpTableNameUtils;

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
        String tableAliasName = tableNameUtils.getRelTableName(tableName);
        sb.append(tableAliasName)
                .append(" (")
                .append(columnDefs)
                .append(")")
                .append(Common.SPACE)
                .append(crudStr);
        if (tableNameUtils.checkIsTmpTable(tableName)) {
            addTmpTable(tableName, tableAliasName);
        }
        commitStatement(sb.toString());
        return 1;
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
