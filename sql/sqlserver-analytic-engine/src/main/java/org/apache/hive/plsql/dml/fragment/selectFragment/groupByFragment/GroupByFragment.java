package org.apache.hive.plsql.dml.fragment.selectFragment.groupByFragment;

import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/19.
 * : GROUP BY group_by_elements (',' group_by_elements)* having_clause?
 * | having_clause (GROUP BY group_by_elements (',' group_by_elements)*)?
 * ;
 */
public class GroupByFragment extends SqlStatement {

    private List<GroupByElemFragment> groupByElemFragments = new ArrayList<>();

    private HavingClauseFragment havingClauseFragment;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(" GROUP BY ");
        sql.append(FragMentUtils.appendOriginalSql(groupByElemFragments,getExecSession()));
        sql.append(Common.SPACE);
        if(null!=havingClauseFragment){
            sql.append(havingClauseFragment.getOriginalSql());
        }
        return  sql.toString();
    }

    public HavingClauseFragment getHavingClauseFragment() {
        return havingClauseFragment;
    }

    public void setHavingClauseFragment(HavingClauseFragment havingClauseFragment) {
        this.havingClauseFragment = havingClauseFragment;
    }

    public void addGroupByElem(GroupByElemFragment groupByElemFragment) {
        groupByElemFragments.add(groupByElemFragment);
    }
}
