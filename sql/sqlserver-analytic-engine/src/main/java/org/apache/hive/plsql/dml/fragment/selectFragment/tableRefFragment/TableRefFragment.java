package org.apache.hive.plsql.dml.fragment.selectFragment.tableRefFragment;

import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.fragment.selectFragment.joinFragment.JoinClauseFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.pivotFragment.PivotClauseFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.unpivotFragment.UnpivotClauseFragment;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/4.
 * table_ref
 * : table_ref_aux join_clause* (pivot_clause | unpivot_clause)?
 * ;
 */
public class TableRefFragment extends SqlStatement {
    private TableRefAuxFragment tableRefAuxFragment;

    private List<JoinClauseFragment> joinClauseFragments = new ArrayList<>();

    private PivotClauseFragment pivotClauseFragment;

    private UnpivotClauseFragment unpivotClauseFragment;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(tableRefAuxFragment.getOriginalSql());
        sql.append(FragMentUtils.appendOriginalSql(joinClauseFragments));
        if (null != pivotClauseFragment) {
            sql.append(pivotClauseFragment.getOriginalSql());
        }
        if (null != unpivotClauseFragment) {
            sql.append(unpivotClauseFragment.getOriginalSql());
        }
        return sql.toString();
    }

    public PivotClauseFragment getPivotClauseFragment() {
        return pivotClauseFragment;
    }

    public void setPivotClauseFragment(PivotClauseFragment pivotClauseFragment) {
        this.pivotClauseFragment = pivotClauseFragment;
    }

    public UnpivotClauseFragment getUnpivotClauseFragment() {
        return unpivotClauseFragment;
    }

    public void setUnpivotClauseFragment(UnpivotClauseFragment unpivotClauseFragment) {
        this.unpivotClauseFragment = unpivotClauseFragment;
    }

    public TableRefAuxFragment getTableRefAuxFragment() {
        return tableRefAuxFragment;
    }

    public void setTableRefAuxFragment(TableRefAuxFragment tableRefAuxFragment) {
        this.tableRefAuxFragment = tableRefAuxFragment;
    }

    public List<JoinClauseFragment> getJoinClauseFragments() {
        return joinClauseFragments;
    }

    public void setJoinClauseFragments(List<JoinClauseFragment> joinClauseFragments) {
        this.joinClauseFragments = joinClauseFragments;
    }


    public void addJoinClauseFragment(JoinClauseFragment joinClauseFragment) {
        joinClauseFragments.add(joinClauseFragment);
    }
}
