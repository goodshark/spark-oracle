package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.hive.plsql.dml.commonFragment.TableViewNameFragment;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

/**
 * select_list_elements
 * : tableview_name '.' '*'
 * | expression
 * <p>
 * Created by wangsm9 on 2017/7/4.
 */
public class SelectListElementsFragment extends SqlStatement {

    private ExpressionStatement expression;
    private TableViewNameFragment tableViewNameFragment;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        if (null != tableViewNameFragment) {
            sql.append(tableViewNameFragment.getOriginalSql());
            sql.append(".*");
        }
        if (null != expression) {
            sql.append(expression.getOriginalSql());
        }

        return sql.toString();
    }

    public ExpressionStatement getExpression() {
        return expression;
    }

    public void setExpression(ExpressionStatement expression) {
        this.expression = expression;
    }

    public TableViewNameFragment getTableViewNameFragment() {
        return tableViewNameFragment;
    }

    public void setTableViewNameFragment(TableViewNameFragment tableViewNameFragment) {
        this.tableViewNameFragment = tableViewNameFragment;
    }
}
