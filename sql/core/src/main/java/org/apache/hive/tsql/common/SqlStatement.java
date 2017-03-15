package org.apache.hive.tsql.common;


import org.apache.commons.lang.StringUtils;

import java.io.Serializable;

/**
 * Created by wangsm9 on 2016/11/24.
 */
public class SqlStatement extends BaseStatement implements Serializable {

    private static final long serialVersionUID = -1531515791432293303L;

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
    public String replaceTableName(String tableName, String sql) throws Exception {
        String realTableName = getRealTableName(tableName);
        return sql.replaceAll(tableName, realTableName);
    }

}
