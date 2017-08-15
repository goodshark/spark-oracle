package org.apache.hive.tsql.sqlsverExcept;


import org.apache.commons.lang.StringUtils;

/**
 * Created by wangsm9 on 2017/3/29.
 */
public class ColumnBean {

    private String cloumnName;
    private String columnAlias;
    private String sql;

    public String getCloumnName() {
        return cloumnName;
    }

    public void setCloumnName(String cloumnName) {
        this.cloumnName = cloumnName;
    }

    public String getColumnAlias() {
        return columnAlias;
    }

    public void setColumnAlias(String columnAlias) {
        this.columnAlias = columnAlias;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getRealColumnName(){
        if(!StringUtils.isBlank(columnAlias)){
            return columnAlias;
        }
        return cloumnName;
    }

    @Override
    public String toString() {
        return sql;
    }
}
