package org.apache.hive.plsql.ddl.fragment.dropTruckTableFm;

import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.commonFragment.TableViewNameFragment;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by zhongdg on 2017/7/28.
 */
public class OracleTruncateTableStatement extends SqlStatement {

    private TableViewNameFragment tableViewNameStatement;


    @Override
    public int execute() throws Exception {
        String finalSql = getFinalSql();
        setRs(commitStatement(finalSql));
        return 0;
    }


    @Override
    public String getFinalSql() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("TRUNCATE TABLE ");
        if (tableViewNameStatement != null) {
            sb.append(FragMentUtils.appendFinalSql(tableViewNameStatement, getExecSession())).append(" ");
        }
        return sb.toString();
    }

    public void setTableViewNameStatement(TableViewNameFragment tableViewNameStatement) {
        this.tableViewNameStatement = tableViewNameStatement;
    }

    public TableViewNameFragment getTableViewNameStatement() {
        return tableViewNameStatement;
    }

}
