package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 6/9 0009.
 */
public class SubqueryFragment extends SqlStatement {
    private SubQuBaseElemFragment basicElement;
    private List<SubqueryOpPartFragment> operaionParts = new ArrayList<>();

    public void setBasicElement(SubQuBaseElemFragment stmt) {
        basicElement = stmt;
    }

    public void addOperation(SubqueryOpPartFragment stmt) {
        operaionParts.add(stmt);
    }


    @Override
    public String getSql() {
        return "";
    }

    @Override
    public String getOriginalSql() {
        StringBuilder sb = new StringBuilder();
        if (basicElement != null)
            sb.append(FragMentUtils.appendOriginalSql(basicElement, getExecSession())).append(" ");
        for (SqlStatement stmt : operaionParts) {
            sb.append(FragMentUtils.appendOriginalSql(stmt, getExecSession())).append(" ");
        }
        return sb.toString();
    }

    @Override
    public String getFinalSql() throws Exception {
        StringBuilder sb = new StringBuilder();
        if (basicElement != null) {
            sb.append(FragMentUtils.appendFinalSql(basicElement, getExecSession())).append(" ");
        }
        for (SqlStatement stmt : operaionParts) {
            stmt.setExecSession(getExecSession());
            sb.append(FragMentUtils.appendFinalSql(stmt, getExecSession())).append(" ");
        }
        return sb.toString();
    }

    public SubQuBaseElemFragment getBasicElement() {
        return basicElement;
    }
}
