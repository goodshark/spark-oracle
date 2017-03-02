package hive.tsql.dml;

import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2016/12/12.
 *
 * 用于求表字段的默认值
 *
 */
public class DefaultValuesStatement extends SqlStatement {

    private String tableName;

    @Override
    public int execute() {
        //TODO 依据表名称获取字段的默认值
        return 0;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
