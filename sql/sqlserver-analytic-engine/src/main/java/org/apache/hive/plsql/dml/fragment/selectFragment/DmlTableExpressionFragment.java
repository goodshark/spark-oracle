package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.hive.plsql.dml.OracleSelectStatement;
import org.apache.hive.plsql.dml.commonFragment.TableViewNameFragment;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/5.
 * <p>
 * dml_table_expression_clause
 * : table_collection_expression
 * | '(' select_statement subquery_restriction_clause? ')'
 * | tableview_name sample_clause?
 * ;
 */
public class DmlTableExpressionFragment extends SqlStatement {


    private TableCollectionExpressionFm tableCollectionExpressionFm;
    private OracleSelectStatement selectStatement;
    private TableViewNameFragment tableViewNameFragment;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        if (null != tableCollectionExpressionFm) {
            sql.append(tableCollectionExpressionFm.getOriginalSql());
        }
        if (null != selectStatement) {
            sql.append(selectStatement.getOriginalSql());
        }
        if (null != tableViewNameFragment) {
            sql.append(tableViewNameFragment.getOriginalSql());
        }
        return sql.toString();
    }

    public TableCollectionExpressionFm getTableCollectionExpressionFm() {
        return tableCollectionExpressionFm;
    }

    public void setTableCollectionExpressionFm(TableCollectionExpressionFm tableCollectionExpressionFm) {
        this.tableCollectionExpressionFm = tableCollectionExpressionFm;
    }

    public OracleSelectStatement getSelectStatement() {
        return selectStatement;
    }

    public void setSelectStatement(OracleSelectStatement selectStatement) {
        this.selectStatement = selectStatement;
    }

    public TableViewNameFragment getTableViewNameFragment() {
        return tableViewNameFragment;
    }

    public void setTableViewNameFragment(TableViewNameFragment tableViewNameFragment) {
        this.tableViewNameFragment = tableViewNameFragment;
    }
}
