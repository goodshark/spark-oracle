package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 *  query_block
 * : SELECT (DISTINCT | UNIQUE | ALL)? ('*' | selected_element (',' selected_element)*)
 * into_clause? from_clause where_clause? hierarchical_query_clause? group_by_clause? model_clause?
 * ;
 *
 *
 *
 * (DISTINCT | UNIQUE | ALL)? ===>queryType
 * if select_element is empty ===>*
 *
 * Created by dengrb1 on 6/9 0009.
 */
public class QueryBlockFragment extends SqlStatement {



    private String quereyType;












    private List<SelectElementFragment> elements = new ArrayList<>();
    private FromClauseFragment fromClause = null;
    private WhereClauseFragment whereClause = null;



    private SqlStatement intoClause = null;
    private SqlStatement hierachyClause = null;
    private SqlStatement groupClause = null;
    private SqlStatement modelClasue = null;

    public void addElement(SelectElementFragment element) {
        elements.add(element);
    }

    public void setIntoClause(SqlStatement stmt) {
        intoClause = stmt;
    }

    public void setFromClause(FromClauseFragment stmt) {
        fromClause = stmt;
    }

    public void setWhereClause(WhereClauseFragment stmt) {
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


    public String getQuereyType() {
        return quereyType;
    }

    public void setQuereyType(String quereyType) {
        this.quereyType = quereyType;
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
