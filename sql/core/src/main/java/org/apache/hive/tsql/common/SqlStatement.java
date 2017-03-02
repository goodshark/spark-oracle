package hive.tsql.common;


import org.apache.commons.lang.StringUtils;

/**
 * Created by wangsm9 on 2016/11/24.
 */
public class SqlStatement extends BaseStatement {
    public SqlStatement() {
    }

    public SqlStatement(String name) {
        super(name);
    }

    @Override
    public int execute() throws Exception {
        setRs(commitStatement(getSql()));
        return 1;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }

    /**
     * 替换sql中的表名
     *
     * @param tableName
     * @param sql
     */
    public String replaceTableName(String tableName, String sql) {
        String realTableName = "";
        TmpTableNameUtils tableNameUtils = new TmpTableNameUtils();
        if (tableName.indexOf("@") != -1) {
            realTableName = findTableVarAlias(tableName);
            return sql.replaceAll(tableName, realTableName);
        } else if (tableNameUtils.checkIsGlobalTmpTable(tableName)) {
            realTableName = tableNameUtils.getRelTableName(tableName);
            return sql.replaceAll(tableName, realTableName);
        } else if(tableNameUtils.checkIsTmpTable(tableName)){
            //临时表，先从内存中找，如果没有找到，需要创建
            realTableName=findTmpTaleAlias(tableName);
            if(StringUtils.isBlank(realTableName)){
                realTableName = tableNameUtils.getRelTableName(tableName);
                addTmpTable(tableName,realTableName);
            }
            return sql.replaceAll(tableName, realTableName);
        } else {
            return sql;
        }

    }

}
