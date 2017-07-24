package org.apache.hive.plsql.dml.fragment.selectFragment.unpivotFragment;

import org.apache.hive.plsql.dml.commonFragment.ColumnNameFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.pivotFragment.PivotForClauseFragment;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * unpivot_clause
 * : UNPIVOT ((INCLUDE | EXCLUDE) NULLS)?
 * '(' (column_name | '(' column_name (',' column_name)* ')') pivot_for_clause unpivot_in_clause ')'
 * ;
 * Created by wangsm9 on 2017/7/4.
 */
public class UnpivotClauseFragment extends SqlStatement {
    private String include;
    private List<ColumnNameFragment> columnNameFragments = new ArrayList<>();
    private PivotForClauseFragment pivotForClauseFragment;
    private UnpivotInClauseFm unpivotInClauseFm;

    public String getInclude() {
        return include;
    }

    public void setInclude(String include) {
        this.include = include;
    }

    public PivotForClauseFragment getPivotForClauseFragment() {
        return pivotForClauseFragment;
    }

    public void setPivotForClauseFragment(PivotForClauseFragment pivotForClauseFragment) {
        this.pivotForClauseFragment = pivotForClauseFragment;
    }

    public UnpivotInClauseFm getUnpivotInClauseFm() {
        return unpivotInClauseFm;
    }

    public void setUnpivotInClauseFm(UnpivotInClauseFm unpivotInClauseFm) {
        this.unpivotInClauseFm = unpivotInClauseFm;
    }

    public void addColumnNames(ColumnNameFragment cnf) {
        columnNameFragments.add(cnf);

    }
}
