package org.apache.hive.tsql.common;

import org.apache.hive.tsql.arg.Var;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

import java.sql.ResultSet;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhongdg1 on 2016/11/29.
 */
public abstract class BaseStatement extends TreeNode {
    private StringBuffer exeSql = new StringBuffer();

    public BaseStatement() {
        super();
    }

    public BaseStatement(String nodeName) {
        this();
        setNodeName(nodeName);
    }

    public String getExeSql() {
        return exeSql.toString();
    }

    /**
     * use sparksession
     *
     * @param exeSql
     * @return
     */
    public ResultSet commitStatement(String exeSql) {
        System.out.println("SparkServer Executing SQL: [" + exeSql + "]");
//        //For testing
        SparkResultSet sparkResultSet = new SparkResultSet();
        sparkResultSet.addColumn(new Column("id", ColumnDataType.INT));
        sparkResultSet.addColumn(new Column("name", ColumnDataType.STRING));
        sparkResultSet.addColumn(new Column("age", ColumnDataType.INT));
        for (int i = 0; i < 10; i++) {
            sparkResultSet.addRow(new Object[]{i*3, "TEST_"+i, i*11});
        }
        if(isAddResult()) {
            getExecSession().addRs(sparkResultSet);
        }

        return sparkResultSet;
        //For testing end

    /*    SparkSession sparkSession = getExecSession().getSparkSession();
        Dataset dataset = sparkSession.sql(exeSql);
        SparkResultSet sparkResultSet = new SparkResultSet(dataset);
        if(isAddResult()) {
            getExecSession().addRs(sparkResultSet);
        }
        return sparkResultSet;*/
    }

    /**
     * build command with some args
     *
     * @return
     */
    public abstract BaseStatement createStatement();

    public int clean() {
        return 0;
    }


    public void addSql(BaseStatement statement) {
        this.exeSql = this.exeSql.append(Common.SEMICOLON).append(statement.exeSql);
    }

    private ConcurrentHashMap<String, Var> cloneVars = new ConcurrentHashMap<String, Var>();
    private Var returnVar = null;

    /**
     * 保存现场
     */
    public void saveScene() {
        cloneVars.clear();
        for (Map.Entry<String, Var> entry : getExecSession().getVariableContainer().getVars().entrySet()) {
            cloneVars.put(entry.getKey(), entry.getValue().clone());
        }
        returnVar = getExecSession().getVariableContainer().getReturnVar().clone();
    }

    /**
     * 恢复现场
     */
    public void recoveryScene() {

//        for (Map.Entry<String, Var> entry : cloneVars.entrySet()) {
////            getExecSession().getVariableContainer().addVar(entry);
//            cloneVars.put(entry.getKey(), getExecSession().getVariableContainer().findVar(entry.getKey()));
//        }
//        getExecSession().getVariableContainer().setVars(cloneVars);
        getExecSession().getVariableContainer().setVars(cloneVars);
//        getExecSession().getVariableContainer().setReturnVar(returnVar.clone());
    }

    public Var getReturnVal() {
        return getExecSession().getVariableContainer().getReturnVar();
    }


    public String productAliasName(String varName) {
        StringBuffer sb = new StringBuffer();
        sb.append("tmp."); //TODO GET FROM HADOOP CONF
        int index = varName.lastIndexOf("@");
        if (-1 != index) {
            sb.append(varName.substring(index + 1)).append("_").append(System.currentTimeMillis()).append("_").append(new Random().nextInt(1000));
        } else {
            sb.append(varName);
        }
        return sb.toString();
    }


}
