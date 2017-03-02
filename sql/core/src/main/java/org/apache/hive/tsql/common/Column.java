package org.apache.hive.tsql.common;

/**
 * Created by zhongdg1 on 2016/12/30.
 */
public class Column {


    private String columnName;
    private String aliasName;
    private ColumnDataType dataType;

    public Column(String columnName, String aliasName, ColumnDataType dataType) {
        this.columnName = columnName;
        this.aliasName = aliasName;
        this.dataType = dataType;
    }

    public Column(String columnName, ColumnDataType dataType) {
        this.columnName = columnName;
        this.dataType = dataType;
    }

    public Column(String columnName) {
        this.columnName = columnName;
    }

    public ColumnDataType getDataType() {
        return dataType;
    }

    public void setDataType(ColumnDataType dataType) {
        this.dataType = dataType;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getAliasName() {
        return aliasName;
    }

    public void setAliasName(String aliasName) {
        this.aliasName = aliasName;
    }
}
