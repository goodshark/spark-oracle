package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * order_by_clause
 * : ORDER SIBLINGS? BY order_by_elements (',' order_by_elements)*
 * <p>
 * Created by wangsm9 on 2017/7/3.
 */
public class OrderByClauseFragment extends SqlStatement {

    private List<OrderByElementsFragment> orderByElements = new ArrayList<>();


    public void addOrderByElem(OrderByElementsFragment orderByElementsFragment) {
        orderByElements.add(orderByElementsFragment);
    }

    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append("order by ");
        sql.append(FragMentUtils.appendOriginalSql(orderByElements, getExecSession()));
        return sql.toString();
    }
}
