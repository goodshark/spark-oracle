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
