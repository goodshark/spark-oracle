package org.apache.hive.tsql.dml.mergeinto;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/4/11.
 */
public class CommonTableExpressionBean extends BaseBean {

    private List<String> columnNameList = new ArrayList<>();

    private String expressionName;

    private String selectsql;

    public List<String> getColumnNameList() {
        return columnNameList;
    }

    public void setColumnNameList(List<String> columnNameList) {
        this.columnNameList = columnNameList;
    }

    public String getExpressionName() {
        return expressionName;
    }

    public void setExpressionName(String expressionName) {
        this.expressionName = expressionName;
    }

    public String getSelectsql() {
        return selectsql;
    }

    public void setSelectsql(String selectsql) {
        this.selectsql = selectsql;
    }
}
