package org.apache.hive.plsql.dml.fragment.mergeFragment;

import org.apache.hive.plsql.dml.OracleSelectStatement;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.commonFragment.TableAliasFragment;
import org.apache.hive.plsql.dml.commonFragment.TableViewNameFragment;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/8/4.
 * selected_tableview
 * : (tableview_name | '(' select_statement ')') table_alias?
 * ;
 */
public class SelectTableViewFragment extends SqlStatement {
    private TableViewNameFragment tableViewNameFragment;
    private OracleSelectStatement selectStatement;
    private TableAliasFragment tableAliasFragment;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        ExecSession execSession = getExecSession();
        if (null != tableViewNameFragment) {
            sql.append(FragMentUtils.appendOriginalSql(tableViewNameFragment, execSession));
        }
        if (null != selectStatement) {
            sql.append("(");
            sql.append(FragMentUtils.appendOriginalSql(selectStatement, execSession));
            sql.append(")");
        }
        if (null != tableAliasFragment) {
            sql.append(FragMentUtils.appendOriginalSql(tableAliasFragment, execSession));
        }
        return sql.toString();
    }

    public TableViewNameFragment getTableViewNameFragment() {
        return tableViewNameFragment;
    }

    public void setTableViewNameFragment(TableViewNameFragment tableViewNameFragment) {
        this.tableViewNameFragment = tableViewNameFragment;
    }

    public OracleSelectStatement getSelectStatement() {
        return selectStatement;
    }

    public void setSelectStatement(OracleSelectStatement selectStatement) {
        this.selectStatement = selectStatement;
    }

    public TableAliasFragment getTableAliasFragment() {
        return tableAliasFragment;
    }

    public void setTableAliasFragment(TableAliasFragment tableAliasFragment) {
        this.tableAliasFragment = tableAliasFragment;
    }
}
