package org.apache.hive.tsql.dml.mergeinto;

/**
 * Created by wangsm9 on 2017/4/11.
 */
public class MatchedBean extends BaseBean {
    public enum MatchedBeanType {
        DEL, UPDATE
    }

    private MatchedBeanType type;

    private String updateSetSql;

    public MatchedBeanType getType() {
        return type;
    }

    public void setType(MatchedBeanType type) {
        this.type = type;
    }

    public String getUpdateSetSql() {
        return updateSetSql;
    }

    public void setUpdateSetSql(String updateSetSql) {
        this.updateSetSql = updateSetSql;
    }
}
