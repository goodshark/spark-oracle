package org.apache.hive.plsql.dml.fragment.selectFragment.pivotFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnNameFragment;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/6.
 *
 * pivot_for_clause
 * : FOR (column_name | '(' column_name (',' column_name)* ')')
 *
 */
public class PivotForClauseFragment extends SqlStatement {

    private List<ColumnNameFragment> columnNameFragments = new ArrayList<>();

    public void addColumn(ColumnNameFragment cnf){
        columnNameFragments.add(cnf);
    }
}
