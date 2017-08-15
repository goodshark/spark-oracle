package org.apache.hive.tsql.dml.mergeinto;

import java.util.List;

/**
 * Created by wangsm9 on 2017/4/11.
 */
public class MergeNotMatchedBean extends BaseBean {
    private List<String> columnNameList;

    private String values;


    public String getValues() {
        return values;
    }

    public void setValues(String values) {
        this.values = values;
    }

    public List<String> getColumnNameList() {
        return columnNameList;
    }

    public void setColumnNameList(List<String> columnNameList) {
        this.columnNameList = columnNameList;
    }
}
