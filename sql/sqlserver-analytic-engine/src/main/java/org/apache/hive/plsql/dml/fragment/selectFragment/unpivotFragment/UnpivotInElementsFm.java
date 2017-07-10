package org.apache.hive.plsql.dml.fragment.selectFragment.unpivotFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnNameFragment;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/10.
 * <p>
 * unpivot_in_elements
 * : (column_name | '(' column_name (',' column_name)* ')')
 * (AS (constant | '(' constant (',' constant)* ')'))?
 */
public class UnpivotInElementsFm extends SqlStatement {

    private List<ColumnNameFragment> columnNameFragments = new ArrayList<>();
    private List<ExpressionStatement> constants = new ArrayList<>();


    public void addConstants(ExpressionStatement expressionStatement) {
        constants.add(expressionStatement);
    }

    public void addColumnNames(ColumnNameFragment columnNameFragment) {
        columnNameFragments.add(columnNameFragment);
    }
}
