package org.apache.hive.tsql.common;


import org.apache.hive.tsql.arg.Var;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Set;

/**
 * Created by wangsm9 on 2016/11/24.
 */
public class SqlStatement extends BaseStatement implements Serializable {

    private static final long serialVersionUID = -1531515791432293303L;
    private static final Logger LOG = LoggerFactory.getLogger(SqlStatement.class);

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
        String realTableName =getExecSession().getRealTableName(tableName);
        return sql.replaceAll(tableName, realTableName);
    }

    public String replaceVariable(String sql, Set<String> variable) throws Exception{
        if (!variable.isEmpty()) {
            for (String s : variable) {
                Var v = s.startsWith("@@") ? findSystemVar(s) : findVar(s);
                if(v==null){
                    LOG.error("variable:" + s + " not defined");
                }else{
                    sql = sql.replaceAll(s, null == v.getVarValue() ? "null": v.getExecString());
                }
            }
        }
        return sql;
    }

}
