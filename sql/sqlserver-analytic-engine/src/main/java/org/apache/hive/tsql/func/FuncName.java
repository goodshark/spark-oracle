package org.apache.hive.tsql.func;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;

/**
 * Created by zhongdg1 on 2016/12/15.
 */
public class FuncName implements Serializable {


    private static final long serialVersionUID = 7735106517206187668L;

    public FuncName() {
    }

    public FuncName(String database, String funcName, String schema) {
        this.database = database;
        this.funcName = funcName;
        this.schema = schema;
    }

    public FuncName(String server, String database, String funcName, String schema) {
        this.server = server;
        this.database = database;
        this.funcName = funcName;
        this.schema = schema;
    }

    private String database;
    private String funcName;
    private String schema;
    private String server;
    private boolean isVariable = false;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void setServer(String str) {
        server = str;
    }

    public String getFuncName() {
        return funcName;
    }

    public void setFuncName(String funcName) {
        this.funcName = funcName;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     *  原始SP名称，[A].[B].[C] --> A.B.C
     * @return
     */
    public String getFullFuncName() {
        StringBuffer sb = new StringBuffer();
//        sb = StringUtils.isNotBlank(this.server) ? sb.append(this.server).append(".") : sb;
       /* if (sb.length() > 0) {000
            sb.append(".");
        }*/
//        sb = StringUtils.isNotBlank(this.database) ? sb.append(this.database).append(".") : sb;
//        sb = StringUtils.isNotBlank(this.schema) ? sb.append(this.schema).append(".") : sb;
       /* if (funcName.startsWith("[") && funcName.endsWith("]")) {
            funcName = funcName.substring(1, funcName.length() - 1);
        }*/
//        sb.append(this.funcName);
        if(StringUtils.isNotBlank(server)) {
            sb.append(server).append(".");
        }
        if(StringUtils.isNotBlank(database)) {
            sb.append(database).append(".");
        }
        if(StringUtils.isNotBlank(schema)) {
            sb.append(schema).append(".");
        }
        if(StringUtils.isNotBlank(funcName)) {
            sb.append(funcName);
        }
        return sb.toString().trim();
    }

    /**
     * SP存储在MYSQL中的名称：
     * [A].[B].[C]-->A.C
     * C-->A.C
     * B.C -->A.C
     * @return
     */
    public String getRealFullFuncName() {
        StringBuffer sb = new StringBuffer();
        sb = StringUtils.isNotBlank(this.database) ? sb.append(bracketTrim(this.database)).append(".") : sb;
        sb.append(getRealFuncName());
        return sb.toString().trim();
    }

    public String getRealFuncName() {
        return bracketTrim(this.funcName);
    }

    private String bracketTrim(String s) {
        String rs = s;
        if (s.startsWith("[")) {
            rs = s.substring(1, s.length());
        }
        if (s.endsWith("]")) {
            rs = rs.substring(0, rs.length() - 1);
        }
        return rs;
    }


    public boolean isVariable() {
        return isVariable;
    }

    public void setVariable(boolean variable) {
        isVariable = variable;
    }
}
