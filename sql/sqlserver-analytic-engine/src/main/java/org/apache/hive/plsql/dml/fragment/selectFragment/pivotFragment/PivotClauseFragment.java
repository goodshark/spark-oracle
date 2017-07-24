package org.apache.hive.plsql.dml.fragment.selectFragment.pivotFragment;

import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/4.
 * pivot_clause
 * : PIVOT XML? '(' pivot_element (',' pivot_element)* pivot_for_clause pivot_in_clause ')'
 * ;
 */
public class PivotClauseFragment extends SqlStatement {

    private String xml;
    private List<PivotElementFragment> pivotElementFragment = new ArrayList<>();
    private PivotForClauseFragment pivotForClauseFragment;
    private PivotInClauseFragment pivotInClauseFragment;

    public void addPivotElement(PivotElementFragment pef) {
        pivotElementFragment.add(pef);
    }

    public String getXml() {
        return xml;
    }

    public void setXml(String xml) {
        this.xml = xml;
    }

    public PivotForClauseFragment getPivotForClauseFragment() {
        return pivotForClauseFragment;
    }

    public void setPivotForClauseFragment(PivotForClauseFragment pivotForClauseFragment) {
        this.pivotForClauseFragment = pivotForClauseFragment;
    }

    public PivotInClauseFragment getPivotInClauseFragment() {
        return pivotInClauseFragment;
    }

    public void setPivotInClauseFragment(PivotInClauseFragment pivotInClauseFragment) {
        this.pivotInClauseFragment = pivotInClauseFragment;
    }
}
