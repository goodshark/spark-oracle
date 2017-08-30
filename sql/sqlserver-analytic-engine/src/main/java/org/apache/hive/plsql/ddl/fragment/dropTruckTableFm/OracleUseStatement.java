package org.apache.hive.plsql.ddl.fragment.dropTruckTableFm;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.commonFragment.IdFragment;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/8/9.
 */
public class OracleUseStatement extends SqlStatement {
    private IdFragment dbName;


    @Override
    public String getFinalSql() throws Exception {
        StringBuffer sql = new StringBuffer();
        sql.append("USE ");
        // oracle 中关键字需要用""
        String database = FragMentUtils.appendFinalSql(dbName, getExecSession());
        sql.append(database.replaceAll("\"","`"));
        return sql.toString();
    }

    @Override
    public int execute() throws Exception {
        String sql = getFinalSql();
        setRs(commitStatement(sql));
        return 0;
    }

    public IdFragment getDbName() {
        return dbName;
    }

    public void setDbName(IdFragment dbName) {
        this.dbName = dbName;
    }
}
