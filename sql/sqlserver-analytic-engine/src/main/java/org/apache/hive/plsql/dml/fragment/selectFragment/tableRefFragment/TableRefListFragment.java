package org.apache.hive.plsql.dml.fragment.selectFragment.tableRefFragment;

import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * table_ref_list
 * : table_ref (',' table_ref)*
 * Created by wangsm9 on 2017/7/4.
 */
public class TableRefListFragment extends SqlStatement {

    private List<TableRefFragment> tableRefFragments = new ArrayList<>();


    public void addFragment(TableRefFragment tableRefFragment) {
        tableRefFragments.add(tableRefFragment);
    }

    @Override
    public String getOriginalSql() {
        return FragMentUtils.appendOriginalSql(tableRefFragments,getExecSession());
    }
}
