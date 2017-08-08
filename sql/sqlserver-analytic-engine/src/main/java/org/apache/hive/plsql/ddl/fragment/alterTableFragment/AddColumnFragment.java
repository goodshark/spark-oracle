package org.apache.hive.plsql.ddl.fragment.alterTableFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnNameFragment;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.commonFragment.TableViewNameFragment;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/27.
 * <p>
 * add_column_clause:
 * ADD column_name type_spec column_constraint?
 */
public class AddColumnFragment extends SqlStatement {
    private TableViewNameFragment tableViewNameFragment;
    private ColumnNameFragment columnNameFragment;
    private Var.DataType dataType;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(" ADD ");
        ExecSession execSession = getExecSession();
        sql.append(FragMentUtils.appendOriginalSql(columnNameFragment, execSession));
        sql.append(Common.SPACE);
        sql.append(dataType.toString());
        sql.append(Common.SPACE);
        return sql.toString();
    }

    public TableViewNameFragment getTableViewNameFragment() {
        return tableViewNameFragment;
    }

    public void setTableViewNameFragment(TableViewNameFragment tableViewNameFragment) {
        this.tableViewNameFragment = tableViewNameFragment;
    }

    public ColumnNameFragment getColumnNameFragment() {
        return columnNameFragment;
    }

    public void setColumnNameFragment(ColumnNameFragment columnNameFragment) {
        this.columnNameFragment = columnNameFragment;
    }

    public Var.DataType getDataType() {
        return dataType;
    }

    public void setDataType(Var.DataType dataType) {
        this.dataType = dataType;
    }
}
