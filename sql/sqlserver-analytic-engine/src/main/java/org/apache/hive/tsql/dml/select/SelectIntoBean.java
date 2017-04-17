package org.apache.hive.tsql.dml.select;

import org.apache.hive.tsql.dml.mergeinto.BaseBean;

/**
 * Created by wangsm9 on 2017/4/17.
 */
public class SelectIntoBean extends BaseBean {

    private String tableNanme;

    public String getTableNanme() {
        return tableNanme;
    }

    public void setTableNanme(String tableNanme) {
        this.tableNanme = tableNanme;
    }
}
