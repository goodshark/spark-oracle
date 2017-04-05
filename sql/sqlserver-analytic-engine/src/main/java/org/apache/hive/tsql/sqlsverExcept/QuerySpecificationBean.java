package org.apache.hive.tsql.sqlsverExcept;


import java.util.List;

/**
 * Created by wangsm9 on 2017/3/29.
 */
public class QuerySpecificationBean {

    private List<ColumnBean> selectList;
    private String sql;
    private String tableName;
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }



    public List<ColumnBean> getSelectList() {
        return selectList;
    }

    public void setSelectList(List<ColumnBean> selectList) {
        this.selectList = selectList;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }


}
