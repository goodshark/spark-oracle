package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnNameFragment;
import org.apache.hive.plsql.dml.commonFragment.IdFragment;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.List;

/**
 * Created by wangsm9 on 2017/6/30.
 */
public class FactoringElementFragment extends SqlStatement {

    /**
     * query_name ('(' column_name (',' column_name)* ')')? AS '(' subquery order_by_clause? ')'
     * search_clause? cycle_clause?
     */
    private IdFragment quereyName;

    private List<ColumnNameFragment> columnNames;

    private SubqueryFragment subquery;

    private OrderByElementsFragment orderByClause;






    public SqlStatement getQuereyName() {
        return quereyName;
    }

    public void setQuereyName(IdFragment quereyName) {
        this.quereyName = quereyName;
    }

    public List<ColumnNameFragment> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(List<ColumnNameFragment> columnNames) {
        this.columnNames = columnNames;
    }

    public SqlStatement getSubquery() {
        return subquery;
    }

    public void setSubquery(SubqueryFragment subquery) {
        this.subquery = subquery;
    }

    public SqlStatement getOrderByClause() {
        return orderByClause;
    }

    public void setOrderByClause(OrderByElementsFragment orderByClause) {
        this.orderByClause = orderByClause;
    }

}
