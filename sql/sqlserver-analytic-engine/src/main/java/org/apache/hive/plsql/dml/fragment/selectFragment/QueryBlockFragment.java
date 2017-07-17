package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * query_block
 * : SELECT (DISTINCT | UNIQUE | ALL)? ('*' | selected_element (',' selected_element)*)
 * into_clause? from_clause where_clause? hierarchical_query_clause? group_by_clause? model_clause?
 * ;
 * <p>
 * <p>
 * <p>
 * (DISTINCT | UNIQUE | ALL)? ===>queryType
 * if select_element is empty ===>*
 * <p>
 * Created by dengrb1 on 6/9 0009.
 */
public class QueryBlockFragment extends SqlStatement {

    private String quereyType;
    private List<SelectElementFragment> elements = new ArrayList<>();
    private FromClauseFragment fromClause;
    private WhereClauseFragment whereClause;


    private IntoClauseFragment intoClause;
    private SqlStatement hierachyClause;
    private SqlStatement groupClause;
    private SqlStatement modelClasue;


    @Override
    public String getSql() {
        return "";
    }

    @Override
    public String getOriginalSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        if (StringUtils.isNotBlank(quereyType)) {
            sb.append(quereyType);
        }
        if (elements.isEmpty()) {
            sb.append(" * ");
        } else {
            sb.append(FragMentUtils.appendOriginalSql(elements));
        }

        if (null != intoClause) {
            sb.append(intoClause.getOriginalSql());
        }
        sb.append(fromClause.getOriginalSql());
        if (null != whereClause) {
            sb.append(whereClause.getOriginalSql());
        }
        return sb.toString();
    }

    public void addElement(SelectElementFragment element) {
        elements.add(element);
    }

    public void setIntoClause(IntoClauseFragment stmt) {
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


}
