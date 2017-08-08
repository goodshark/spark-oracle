package org.apache.hive.plsql.ddl.fragment.dropTruckTableFm;

import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.commonFragment.TableViewNameFragment;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by zhongdg on 2017/7/28.
 */
public class OracleDropViewStatement extends SqlStatement {

    private TableViewNameFragment viewNameStatement;

    public OracleDropViewStatement(String nodeName) {
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
        sb.append("DROP VIEW ");
        if (viewNameStatement != null) {
            viewNameStatement.setExecSession(getExecSession());
            sb.append(FragMentUtils.appendFinalSql(viewNameStatement, getExecSession())).append(" ");
        }
        return sb.toString();
    }

    public void setviewNameStatement(TableViewNameFragment viewNameStatement) {
        this.viewNameStatement = viewNameStatement;
    }

    public TableViewNameFragment getviewNameStatement() {
        return viewNameStatement;
    }

}
