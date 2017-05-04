package org.apache.hive.tsql.dbservice;

import org.apache.hive.tsql.func.Procedure;

/**
 * Created by wangsm9 on 2017/5/3.
 */
public class ProcBean {
    private String procName;

    private String md5;

    private String procContent;

    private Procedure procObject;

    private String dbName;

    private String userName;

    private String procOrcName;

    private String type;

    public String getProcName() {
        return procName;
    }

    public void setProcName(String procName) {
        this.procName = procName;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public String getProcContent() {
        return procContent;
    }

    public void setProcContent(String procContent) {
        this.procContent = procContent;
    }

    public Procedure getProcObject() {
        return procObject;
    }

    public void setProcObject(Procedure procObject) {
        this.procObject = procObject;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getProcOrcName() {
        return procOrcName;
    }

    public void setProcOrcName(String procOrcName) {
        this.procOrcName = procOrcName;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}
