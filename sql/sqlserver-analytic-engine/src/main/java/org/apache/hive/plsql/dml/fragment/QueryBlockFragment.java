package org.apache.hive.plsql.dml.fragment;

import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 6/9 0009.
 */
public class QueryBlockFragment extends SqlStatement {
    private List<SqlStatement> elements = new ArrayList<>();
    private SqlStatement intoClause = null;
    private SqlStatement fromClause = null;
    private SqlStatement whereClause = null;
    private SqlStatement hierachyClause = null;
    private SqlStatement groupClause = null;
    private SqlStatement modelClasue = null;

    public void addElement(SqlStatement element) {
        elements.add(element);
    }

    public void setIntoClause(SqlStatement stmt) {
        intoClause = stmt;
    }

    public void setFromClause(SqlStatement stmt) {
        fromClause = stmt;
    }

    public void setWhereClause(SqlStatement stmt) {
        whereClause = stmt;
    }

    public void setHierachyClause(SqlStatement stmt) {
        hierachyClause = stmt;
    }

    public void setGroupClause(SqlStatement stmt) {
        groupClause = stmt;
    }

    public void setModelClasue(SqlStatement stmt) {
        modelClasue = stmt;
    }

    @Override
    public String getSql() {
        return "";
    }

    @Override
    public String getOriginalSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        for (SqlStatement element: elements) {
            sb.append(element.getOriginalSql()).append(" ");
        }
        if (fromClause != null)
            sb.append(fromClause.getOriginalSql());
        return sb.toString();
    }

    @Override
    public String getFinalSql() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        /*for (SqlStatement element: elements) {
            element.setExecSession(getExecSession());
            sb.append(element.getFinalSql()).append(" ");
        }*/
        for (int i = 0; i < elements.size(); i++) {
            elements.get(i).setExecSession(getExecSession());
            sb.append(elements.get(i).getFinalSql());
            if (i < elements.size() - 1)
                sb.append(", ");
            else
                sb.append(" ");
        }
        if (fromClause != null) {
            fromClause.setExecSession(getExecSession());
            sb.append(fromClause.getFinalSql()).append(" ");
        }

        if (whereClause != null) {
            whereClause.setExecSession(getExecSession());
            sb.append(whereClause.getFinalSql()).append(" ");
        }
        return sb.toString();
    }
}
