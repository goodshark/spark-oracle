package org.apache.hive.tsql.dml.mergeinto;

/**
 * Created by wangsm9 on 2017/4/11.
 */
public class LimitBean extends BaseBean {
    private  String expression;

    @Override
    public String getSql() {
        String sql=" limit "+ expression+" ";
        return sql;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }
}
