package org.apache.hive.plsql.dml.fragment.selectFragment.tableRefFragment;

import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.commonFragment.TableAliasFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.DmlTableExpressionFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.SubqueryOpPartFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.pivotFragment.PivotClauseFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.unpivotFragment.UnpivotClauseFragment;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/4.
 * table_ref_aux
 * : (dml_table_expression_clause (pivot_clause | unpivot_clause)?
 * | '(' table_ref subquery_operation_part* ')' (pivot_clause | unpivot_clause)?
 * | ONLY '(' dml_table_expression_clause ')'
 * | dml_table_expression_clause (pivot_clause | unpivot_clause)?)
 * flashback_query_clause* ( table_alias)?
 */
public class TableRefAuxFragment extends SqlStatement {

    private String only;
    private PivotClauseFragment pivotClauseFragment;
    private UnpivotClauseFragment unpivotClauseFragment;
    private TableRefFragment tableRefFragment;
    private DmlTableExpressionFragment dmlTableExpressionFragment;
    private List<SubqueryOpPartFragment> subqueryOpPartFragment = new ArrayList<>();
    private TableAliasFragment tableAliasFragment;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        if (null != dmlTableExpressionFragment) {
            sql.append(FragMentUtils.appendOriginalSql(dmlTableExpressionFragment, getExecSession()));
            getPivotSql(sql);

        }
        if (null != tableRefFragment) {
            sql.append(FragMentUtils.appendOriginalSql(tableRefFragment, getExecSession()));
            sql.append(FragMentUtils.appendOriginalSql(subqueryOpPartFragment, getExecSession()));
            getPivotSql(sql);

        }

        if (null != tableAliasFragment) {
            sql.append(FragMentUtils.appendOriginalSql(tableAliasFragment, getExecSession()));
        }
        return sql.toString();

    }

    private void getPivotSql(StringBuffer sql) {
        if (null != unpivotClauseFragment) {
            sql.append(FragMentUtils.appendOriginalSql(unpivotClauseFragment, getExecSession()));
        }
        if (null != pivotClauseFragment) {
            sql.append(FragMentUtils.appendOriginalSql(pivotClauseFragment, getExecSession()));
        }
        sql.append(Common.SPACE);
    }


    public TableAliasFragment getTableAliasFragment() {
        return tableAliasFragment;
    }

    public void setTableAliasFragment(TableAliasFragment tableAliasFragment) {
        this.tableAliasFragment = tableAliasFragment;
    }

    public String getOnly() {
        return only;
    }

    public void setOnly(String only) {
        this.only = only;
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

    public TableRefFragment getTableRefFragment() {
        return tableRefFragment;
    }

    public void setTableRefFragment(TableRefFragment tableRefFragment) {
        this.tableRefFragment = tableRefFragment;
    }

    public DmlTableExpressionFragment getDmlTableExpressionFragment() {
        return dmlTableExpressionFragment;
    }

    public void setDmlTableExpressionFragment(DmlTableExpressionFragment dmlTableExpressionFragment) {
        this.dmlTableExpressionFragment = dmlTableExpressionFragment;
    }

    public List<SubqueryOpPartFragment> getSubqueryOpPartFragment() {
        return subqueryOpPartFragment;
    }

    public void setSubqueryOpPartFragment(List<SubqueryOpPartFragment> subqueryOpPartFragment) {
        this.subqueryOpPartFragment = subqueryOpPartFragment;
    }
}
