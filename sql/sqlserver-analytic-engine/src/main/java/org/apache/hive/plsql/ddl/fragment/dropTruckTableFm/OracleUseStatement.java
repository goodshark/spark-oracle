package org.apache.hive.plsql.ddl.fragment.dropTruckTableFm;

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
        sql.append(FragMentUtils.appendFinalSql(dbName, getExecSession()));
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
