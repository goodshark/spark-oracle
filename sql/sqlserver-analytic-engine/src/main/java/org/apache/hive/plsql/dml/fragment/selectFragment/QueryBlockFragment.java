package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.fragment.selectFragment.groupByFragment.GroupByFragment;
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

    private GroupByFragment groupClause;


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
            sb.append(FragMentUtils.appendOriginalSql(elements, getExecSession()));
        }

        if (null != intoClause) {
            sb.append(FragMentUtils.appendOriginalSql(intoClause, getExecSession()));
        }
        sb.append(FragMentUtils.appendOriginalSql(fromClause, getExecSession()));
        if (null != whereClause) {
            sb.append(FragMentUtils.appendOriginalSql(whereClause, getExecSession()));
        }
        if (null != groupClause) {
            sb.append(FragMentUtils.appendOriginalSql(groupClause, getExecSession()));
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

    public GroupByFragment getGroupClause() {
        return groupClause;
    }

    public void setGroupClause(GroupByFragment groupClause) {
        this.groupClause = groupClause;
    }


    public String getQuereyType() {
        return quereyType;
    }

    public void setQuereyType(String quereyType) {
        this.quereyType = quereyType;
    }


    public List<SelectElementFragment> getSelectElements() {
        return elements;
    }

    public void replaceSelectElem(List<SelectElementFragment> selectElem) {
        elements.clear();
        elements.addAll(selectElem);
    }

    public IntoClauseFragment getIntoClause() {
        return intoClause;
    }
}
