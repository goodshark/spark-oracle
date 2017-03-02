package hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhongdg1 on 2016/12/12.
 */
public class Procedure implements Serializable {
    private static final long serialVersionUID = 1227077492869753299L;
    private FuncName name;
    private boolean isGlobal;
    private String md5;
    private String dbName;

    //    private List<Var> inputs;
//    private List<Var> outputs;
    private List<Var> inAndOutputs;
    private TreeNode sqlClauses;
    private String procSql;
    private int procSource = 0;// 来源，0表示内存 1表示从数据库读取
    private int leastArguments = 0;


    public void setLeastArguments(int leastArguments) {
        this.leastArguments = leastArguments;
    }

    public int getLeastArguments() {
        return leastArguments;
    }

    public Procedure(FuncName name) {
        this(name, false, null);
    }

    public Procedure(FuncName name, boolean isGlobal, String md5) {
        this.name = name;
        this.isGlobal = isGlobal;
        this.md5 = md5;
//        inputs = new ArrayList<Var>();
//        outputs = new ArrayList<Var>();
        inAndOutputs = new ArrayList<Var>();
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

//    public List<Var> getInputs() {
//        return inputs;
//    }
//
//    public void setInputs(List<Var> inputs) {
//        this.inputs = inputs;
//    }
//
//    public List<Var> getOutputs() {
//        return outputs;
//    }
//
//    public void setOutputs(List<Var> outputs) {
//        this.outputs = outputs;
//    }

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
