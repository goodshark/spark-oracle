package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.hive.tsql.common.SqlStatement;

/**
 * subquery_operation_part
 * : (UNION ALL? | INTERSECT | MINUS) subquery_basic_elements
 * Created by wangsm9 on 2017/7/3.
 */
public class SubqueryOpPartFragment extends SqlStatement {

    private String op;

    private SubQuBaseElemFragment subQuBaseElemFragment;

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public SubQuBaseElemFragment getSubQuBaseElemFragment() {
        return subQuBaseElemFragment;
    }

    public void setSubQuBaseElemFragment(SubQuBaseElemFragment subQuBaseElemFragment) {
        this.subQuBaseElemFragment = subQuBaseElemFragment;
    }
}
