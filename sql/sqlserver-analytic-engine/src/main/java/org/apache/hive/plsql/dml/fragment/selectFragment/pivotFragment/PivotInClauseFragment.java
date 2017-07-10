package org.apache.hive.plsql.dml.fragment.selectFragment.pivotFragment;

import org.apache.hive.plsql.dml.fragment.selectFragment.SubqueryFragment;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/6.
 * <p>
 * pivot_in_clause
 * : IN '(' (subquery | ANY (',' ANY)* | pivot_in_clause_element (',' pivot_in_clause_element)*) ')'
 */
public class PivotInClauseFragment extends SqlStatement {


    private SubqueryFragment subqueryFragment = new SubqueryFragment();
    private List<PivotInClauseElemetFm> pivotInClauseElemetFms = new ArrayList<>();

    private List<String> anys = new ArrayList<>();

    public void addPivotInClauseFms(PivotInClauseElemetFm pivotInClauseElemetFm) {
        pivotInClauseElemetFms.add(pivotInClauseElemetFm);
    }

    public void addAnys(String any) {
        anys.add(any);
    }

    public SubqueryFragment getSubqueryFragment() {
        return subqueryFragment;
    }

    public void setSubqueryFragment(SubqueryFragment subqueryFragment) {
        this.subqueryFragment = subqueryFragment;
    }

}
