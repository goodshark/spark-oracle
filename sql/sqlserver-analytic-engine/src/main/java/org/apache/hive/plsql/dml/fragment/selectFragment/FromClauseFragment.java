package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.hive.plsql.dml.fragment.selectFragment.tableRefFragment.TableRefListFragment;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by dengrb1 on 6/9 0009.
 *
 *
 * from_clause
 : FROM table_ref_list
 ;
 */
public class FromClauseFragment extends SqlStatement {
    private TableRefListFragment sourceFrag = null;

    public void setSourceFrag(TableRefListFragment stmt) {
        sourceFrag = stmt;
    }

    @Override
    public String getSql() {
        return "";
    }

    @Override
    public String getOriginalSql() {
        return " FROM " + (sourceFrag == null ? "" : sourceFrag.getOriginalSql());
    }

    @Override
    public String getFinalSql() throws Exception {
        return " FROM " + (sourceFrag == null ? "" : sourceFrag.getFinalSql());
    }
}
