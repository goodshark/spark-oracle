package org.apache.hive.plsql.dml.fragment.selectFragment.joinFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnNameFragment;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/5.
 * join_using_part
 * : USING '(' column_name (',' column_name)* ')'
 */
public class JoinUsingPartFragment extends SqlStatement {
    private List<ColumnNameFragment> columnNames = new ArrayList<>();

    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(" USING  ");
        sql.append(FragMentUtils.appendOriginalSql(columnNames, getExecSession()));
        return sql.toString();
    }



    public void addColumns(ColumnNameFragment columnName) {
        columnNames.add(columnName);
    }

}
