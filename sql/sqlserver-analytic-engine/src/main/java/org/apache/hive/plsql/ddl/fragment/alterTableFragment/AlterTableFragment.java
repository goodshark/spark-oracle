package org.apache.hive.plsql.ddl.fragment.alterTableFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnNameFragment;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.commonFragment.TableViewNameFragment;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/26.
 : ALTER TABLE tableview_name (add_column_clause
 | modify_column_clause
 | drop_column_clause
 | rename_column_clause)
 */

public class AlterTableFragment extends SqlStatement {

    private TableViewNameFragment tableViewNameFragment;
    private AddColumnFragment addColumnFragment;
    private DropColumnFragment dropColumnFragment;
    private ModifyColumnFragment modifyColumnFragment;
    private ReNameColumnFragment reNameColumnFragment;


    @Override
    public int execute() throws Exception {
        String sql = getOriginalSql();
        setRs(commitStatement(sql));
        return 0;
    }

    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append("ALTER TABLE ");
        sql.append(FragMentUtils.appendOriginalSql(tableViewNameFragment, getExecSession()));
        if (null != addColumnFragment) {
            sql.append(FragMentUtils.appendOriginalSql(addColumnFragment, getExecSession()));
        }
        if (null != modifyColumnFragment) {
            sql.append(FragMentUtils.appendOriginalSql(modifyColumnFragment, getExecSession()));
        }
        if (null != reNameColumnFragment) {
            sql.append(FragMentUtils.appendOriginalSql(reNameColumnFragment, getExecSession()));
        }
        if (null != dropColumnFragment) {
            sql.append(FragMentUtils.appendOriginalSql(dropColumnFragment, getExecSession()));
        }
        return sql.toString();
    }

    public AddColumnFragment getAddColumnFragment() {
        return addColumnFragment;
    }

    public void setAddColumnFragment(AddColumnFragment addColumnFragment) {
        this.addColumnFragment = addColumnFragment;
    }

    public DropColumnFragment getDropColumnFragment() {
        return dropColumnFragment;
    }

    public void setDropColumnFragment(DropColumnFragment dropColumnFragment) {
        this.dropColumnFragment = dropColumnFragment;
    }

    public ModifyColumnFragment getModifyColumnFragment() {
        return modifyColumnFragment;
    }

    public void setModifyColumnFragment(ModifyColumnFragment modifyColumnFragment) {
        this.modifyColumnFragment = modifyColumnFragment;
    }

    public ReNameColumnFragment getReNameColumnFragment() {
        return reNameColumnFragment;
    }

    public void setReNameColumnFragment(ReNameColumnFragment reNameColumnFragment) {
        this.reNameColumnFragment = reNameColumnFragment;
    }

    public TableViewNameFragment getTableViewNameFragment() {
        return tableViewNameFragment;
    }

    public void setTableViewNameFragment(TableViewNameFragment tableViewNameFragment) {
        this.tableViewNameFragment = tableViewNameFragment;
    }
}
