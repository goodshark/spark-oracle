package org.apache.hive.plsql.ddl.fragment.alterTableFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnNameFragment;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/27.
 * rename_column_clause:
 * RENAME column_name TO column_name
 * ;
 */
public class ReNameColumnFragment extends SqlStatement {
    private ColumnNameFragment srcColumnNameFragment;
    private ColumnNameFragment newColumnNameFragment;

    public ColumnNameFragment getSrcColumnNameFragment() {
        return srcColumnNameFragment;
    }

    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(" CHANGE ");
        ExecSession execSession = getExecSession();
        sql.append(FragMentUtils.appendOriginalSql(srcColumnNameFragment, execSession));
        sql.append(Common.SPACE);
        sql.append(FragMentUtils.appendOriginalSql(newColumnNameFragment, execSession));
        sql.append(Common.SPACE);
        return sql.toString();
    }


    public void setSrcColumnNameFragment(ColumnNameFragment srcColumnNameFragment) {
        this.srcColumnNameFragment = srcColumnNameFragment;
    }

    public ColumnNameFragment getNewColumnNameFragment() {
        return newColumnNameFragment;
    }

    public void setNewColumnNameFragment(ColumnNameFragment newColumnNameFragment) {
        this.newColumnNameFragment = newColumnNameFragment;
    }
}
