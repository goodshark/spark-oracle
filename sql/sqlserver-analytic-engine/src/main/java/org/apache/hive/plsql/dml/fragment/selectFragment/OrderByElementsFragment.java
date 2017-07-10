package org.apache.hive.plsql.dml.fragment.selectFragment;

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
