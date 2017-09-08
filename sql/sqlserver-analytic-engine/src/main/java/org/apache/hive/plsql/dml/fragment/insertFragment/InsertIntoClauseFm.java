package org.apache.hive.plsql.dml.fragment.insertFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnNameFragment;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.fragment.selectFragment.tableRefFragment.GeneralTableRefFragment;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/11.
 * insert_into_clause
 * : INTO general_table_ref ('(' column_name (',' column_name)* ')')?
 * ;
 */
public class InsertIntoClauseFm extends SqlStatement {
    private GeneralTableRefFragment generalTableRefFragment;
    private List<ColumnNameFragment> columnNames = new ArrayList<>();


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(" INTO ");
        ExecSession execSession = getExecSession();
        sql.append(FragMentUtils.appendOriginalSql(generalTableRefFragment, execSession));
        if(!columnNames.isEmpty()){
            sql.append("(");
            sql.append(FragMentUtils.appendOriginalSql(columnNames, execSession));
            sql.append(")");
        }
        return sql.toString();
    }

    public void addColumnName(ColumnNameFragment columnNameFragment) {
        columnNames.add(columnNameFragment);
    }

    public GeneralTableRefFragment getGeneralTableRefFragment() {
        return generalTableRefFragment;
    }

    public void setGeneralTableRefFragment(GeneralTableRefFragment generalTableRefFragment) {
        this.generalTableRefFragment = generalTableRefFragment;
    }
}
