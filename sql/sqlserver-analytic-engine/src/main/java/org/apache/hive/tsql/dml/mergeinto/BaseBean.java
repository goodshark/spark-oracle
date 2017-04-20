package org.apache.hive.tsql.dml.mergeinto;

import java.io.Serializable;

/**
 * Created by wangsm9 on 2017/4/11.
 */
public class BaseBean implements Serializable{

    private static final long serialVersionUID = 2488450665907099010L;
    private String sql;

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
