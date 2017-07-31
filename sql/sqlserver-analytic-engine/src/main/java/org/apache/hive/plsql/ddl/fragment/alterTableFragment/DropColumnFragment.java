package org.apache.hive.plsql.ddl.fragment.alterTableFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnNameFragment;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/27.
 * drop_column_clause:
 * DROP COLUMN column_name
 */
public class DropColumnFragment extends SqlStatement {
    private ColumnNameFragment columnNameFragment;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(" DROP COLUMN ");
        ExecSession execSession = getExecSession();
        sql.append(FragMentUtils.appendOriginalSql(columnNameFragment, execSession));
        return sql.toString();
    }


    public ColumnNameFragment getColumnNameFragment() {
        return columnNameFragment;
    }

    public void setColumnNameFragment(ColumnNameFragment columnNameFragment) {
        this.columnNameFragment = columnNameFragment;
    }
}
