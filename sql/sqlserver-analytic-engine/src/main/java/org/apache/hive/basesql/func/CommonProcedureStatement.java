package org.apache.hive.basesql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.func.FuncName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 5/25 0025.
 */
public abstract class CommonProcedureStatement implements Serializable {
    private static final long serialVersionUID = 1227077492869753299L;
    private FuncName name;
    private boolean isGlobal;
    private String md5;
    private String dbName;

    private List<Var> inAndOutputs;
    private TreeNode sqlClauses;
    private String procSql;
    private int procSource = 0;// 来源，0表示内存 1表示从数据库读取
    private int leastArguments = 0;

    public CommonProcedureStatement(FuncName name) {
        this(name, false, null);
    }

    public CommonProcedureStatement(FuncName name, boolean isGlobal, String md5) {
        this.name = name;
        this.isGlobal = isGlobal;
        this.md5 = md5;
        inAndOutputs = new ArrayList<Var>();
    }

    public void setLeastArguments() {
        int count = 0;
        for (Var arg: inAndOutputs) {
            if (!arg.isDefault())
                count++;
        }
        leastArguments = count;
    }

    public int getLeastArguments() {
        return leastArguments;
    }

    public void addInAndOutputs(Var var) {
        this.inAndOutputs.add(var);
        if(!var.isDefault()) {
            leastArguments = inAndOutputs.size();
        }
    }

    public List<Var> getInAndOutputs() {
        return inAndOutputs;
    }

    public void setSqlClauses(TreeNode sqlClauses) {
        this.sqlClauses = sqlClauses;
    }

    public TreeNode getSqlClauses() {
        return sqlClauses;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public boolean isGlobal() {
        return isGlobal;
    }

    public void setGlobal(boolean global) {
        isGlobal = global;
    }

    public String getMd5() {
        return md5;
    }

    public void setMd5(String md5) {
        this.md5 = md5;
    }

    public FuncName getName() {
        return name;
    }

    public void setName(FuncName name) {
        this.name = name;
    }

    public int getProcSource() {
        return procSource;
    }

    public void setProcSource(int procSource) {
        this.procSource = procSource;
    }

    public void setProcSql(String procSql) {
        this.procSql = procSql;
    }

    public String getProcSql() {
        return procSql;
    }
}
