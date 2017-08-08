package org.apache.hive.plsql.dml.fragment.mergeFragment;

import org.apache.hive.plsql.dml.fragment.selectFragment.WhereClauseFragment;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/8/4.
 * merge_update_delete_part
 * : DELETE where_clause
 * ;
 */
public class MergerUpdateDelFragment extends SqlStatement {

    private WhereClauseFragment whereClauseFragment;

    public WhereClauseFragment getWhereClauseFragment() {
        return whereClauseFragment;
    }

    public void setWhereClauseFragment(WhereClauseFragment whereClauseFragment) {
        this.whereClauseFragment = whereClauseFragment;
    }
}
