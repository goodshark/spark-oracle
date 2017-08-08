package org.apache.hive.plsql.ddl.fragment.dropTruckTableFm;


import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.commonFragment.TableViewNameFragment;
import org.apache.hive.tsql.common.SqlStatement;

public class OracleDropTableStatement extends SqlStatement {


    private TableViewNameFragment tableNameStatement;

    public OracleDropTableStatement(String nodeName) {
        super(nodeName);
        setAddResult(false);
    }

    @Override
    public int execute() throws Exception {
        String finalSql = getFinalSql();
        setRs(commitStatement(finalSql));
        return 0;
    }


    @Override
    public String getFinalSql() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP TABLE ");
        if (tableNameStatement != null) {
            sb.append(FragMentUtils.appendFinalSql(tableNameStatement, getExecSession())).append(" ");
        }
        return sb.toString();
    }

    public void settableNameStatement(TableViewNameFragment tableNameStatement) {
        this.tableNameStatement = tableNameStatement;
    }

    public TableViewNameFragment gettableNameStatement() {
        return tableNameStatement;
    }
}
