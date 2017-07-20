package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

/**
 * order_by_elements
 * : expression (ASC | DESC)? (NULLS (FIRST | LAST))?
 * ;
 * Created by wangsm9 on 2017/7/3.
 */
public class OrderByElementsFragment extends SqlStatement {

    private ExpressionStatement expression;
    private String orderType;
    private String nullOrderType;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(FragMentUtils.appendOriginalSql(expression,getExecSession()));
        sql.append(Common.SPACE);
        if (StringUtils.isNotBlank(orderType)) {
            sql.append(orderType);
        }
        sql.append(Common.SPACE);
        if (StringUtils.isNotBlank(nullOrderType)) {
            sql.append(nullOrderType);
        }
        return sql.toString();
    }

    public SqlStatement getExpression() {
        return expression;
    }

    public void setExpression(ExpressionStatement expression) {
        this.expression = expression;
    }

    public String getOrderType() {
        return orderType;
    }

    public void setOrderType(String orderType) {
        this.orderType = orderType;
    }

    public String getNullOrderType() {
        return nullOrderType;
    }

    public void setNullOrderType(String nullOrderType) {
        this.nullOrderType = nullOrderType;
    }
}
