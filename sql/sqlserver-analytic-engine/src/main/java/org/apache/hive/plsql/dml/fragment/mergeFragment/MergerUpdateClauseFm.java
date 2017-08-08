package org.apache.hive.plsql.dml.fragment.mergeFragment;

import org.apache.hive.plsql.dml.fragment.selectFragment.WhereClauseFragment;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/8/4.
 * merge_update_clause
 * : WHEN MATCHED THEN UPDATE SET merge_element (',' merge_element)* where_clause? merge_update_delete_part?
 * ;
 */
public class MergerUpdateClauseFm extends SqlStatement {

    private List<MergerElementFragement> mergerElementFragements = new ArrayList<>();
    private WhereClauseFragment whereClauseFragment;
    private MergerUpdateDelFragment mergerUpdateDelFragment;

    public WhereClauseFragment getWhereClauseFragment() {
        return whereClauseFragment;
    }

    public void setWhereClauseFragment(WhereClauseFragment whereClauseFragment) {
        this.whereClauseFragment = whereClauseFragment;
    }

    public MergerUpdateDelFragment getMergerUpdateDelFragment() {
        return mergerUpdateDelFragment;
    }

    public void setMergerUpdateDelFragment(MergerUpdateDelFragment mergerUpdateDelFragment) {
        this.mergerUpdateDelFragment = mergerUpdateDelFragment;
    }

    public void addMergerElem(MergerElementFragement elementFragement) {
        mergerElementFragements.add(elementFragement);
    }

    public List<MergerElementFragement> getMergerElementFragements() {
        return mergerElementFragements;
    }

    public void setMergerElementFragements(List<MergerElementFragement> mergerElementFragements) {
        this.mergerElementFragements = mergerElementFragements;
    }
}
