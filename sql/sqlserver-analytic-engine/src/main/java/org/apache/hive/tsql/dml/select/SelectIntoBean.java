package org.apache.hive.tsql.dml.select;

import org.apache.hive.tsql.dml.mergeinto.BaseBean;

/**
 * Created by wangsm9 on 2017/4/17.
 */
public class SelectIntoBean extends BaseBean {

    private String intoTableName;


    private String sourceTableName;

    private String clusterByColumnName;

    private  boolean procFlag;





    public String getIntoTableName() {
        return intoTableName;
    }

    public void setIntoTableName(String intoTableName) {
        this.intoTableName = intoTableName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public String getClusterByColumnName() {
        return clusterByColumnName;
    }

    public void setClusterByColumnName(String clusterByColumnName) {
        this.clusterByColumnName = clusterByColumnName;
    }

    public boolean isProcFlag() {
        return procFlag;
    }

    public void setProcFlag(boolean procFlag) {
        this.procFlag = procFlag;
    }
}
