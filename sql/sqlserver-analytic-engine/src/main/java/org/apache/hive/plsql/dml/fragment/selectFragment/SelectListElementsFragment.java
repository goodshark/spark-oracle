package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.commonFragment.TableViewNameFragment;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(SelectListElementsFragment.class);


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        if (null != tableViewNameFragment) {
            sql.append(FragMentUtils.appendOriginalSql(tableViewNameFragment, getExecSession()));
            sql.append(".*");
        }
        if (null != expression) {
            try {
                sql.append(FragMentUtils.appendFinalSql(expression, getExecSession()));
            } catch (Exception e) {
                LOG.error("get ExpressionStatement sql error ", e);
            }
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
