package org.apache.hive.tsql.common;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.tsql.arg.SystemVName;
import org.apache.hive.tsql.arg.Var;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhongdg1 on 2016/11/29.
 */
public abstract class BaseStatement extends TreeNode {
    private static final Logger LOG = LoggerFactory.getLogger(BaseStatement.class);
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
//        SparkResultSet sparkResultSet = new SparkResultSet();
//        sparkResultSet.addColumn(new Column("id", ColumnDataType.INT));
//        sparkResultSet.addColumn(new Column("name", ColumnDataType.STRING));
////        sparkResultSet.addColumn(new Column("age", ColumnDataType.INT));
//        for (int i = 0; i < 10; i++) {
//            sparkResultSet.addRow(new Object[]{i * 3, "TEST_" + i});
//        }

//        return sparkResultSet;
        //For testing end
        SparkSession sparkSession = getExecSession().getSparkSession();
        LogicalPlan plan = sparkSession.sqlContext().sessionState().sqlParser().parsePlan(exeSql);
        getExecSession().addLogicalPlans(plan);
        Dataset dataset = sparkSession.sql(exeSql);
        SparkResultSet sparkResultSet = new SparkResultSet(dataset);
        updateRowcount(sparkResultSet);
        if (isAddResult()) {
//            if (null != getForClause()) {
//                String ret = formatResultSet(sparkResultSet);
//                Dataset newDs = sparkSession.sql(new StringBuffer().append("SELECT '").append(ret).append("' AS xml").toString());
//                SparkResultSet newRs = new SparkResultSet(newDs);
//                getExecSession().addRs(newRs);
//                return newRs;
//            }
            getExecSession().addRs(sparkResultSet);
        }

        return sparkResultSet;
    }

    private String formatResultSet(SparkResultSet sparkResultSet) {
        StringBuffer sb = new StringBuffer();
        try {

            ForClause forClause = getForClause();
            if (forClause.getDirectives() == ForClause.DIRECTIVES.ROOT) {
                sb.append("<root>");
            }
            int columnSize = sparkResultSet.getColumnSize();
            List<Column> columns = sparkResultSet.getColumns();
            while (sparkResultSet.next()) {
                Row row = sparkResultSet.fetchRow();
                if (StringUtils.isNotBlank(forClause.getRow())) {
                    sb.append("<").append(forClause.getRow()).append(">");
                }

                for (int i = 0; i < columnSize; i++) {
                    sb.append("<").append(columns.get(i).getColumnName()).append(">")
                            .append(row.getColumnVal(i)).append("</").append(columns.get(i).getColumnName()).append(">");
                }
                if (StringUtils.isNotBlank(forClause.getRow())) {
                    sb.append("</").append(forClause.getRow()).append(">");
                }
            }
            if (forClause.getDirectives() == ForClause.DIRECTIVES.ROOT) {
                sb.append("</root>");
            }


//          newResult.addColumn(new Column("xml", ColumnDataType.STRING));
//          newResult.addRow(new Object[]{sb.toString()});

//            StructType st = new StructType().add(new StructField("c", StringType, true, Metadata.empty()));
//            List list = new ArrayList<org.apache.spark.sql.Row>();
//            list.add(new GenericInternalRow(new Object[]{sb.toString()}));
//            list.add(new GenericRowWithSchema(new Object[]{sb.toString()}, st));
//            Dataset ds = getExecSession().getSparkSession().createDataset(list, Encoders.STRING());

//            Dataset<org.apache.spark.sql.Row> ds = getExecSession().getSparkSession().createDataFrame(list, st);
//            SparkResultSet rs = new SparkResultSet();
//            sparkResultSet.setDataset(ds);
//            return rs;

        } catch (Exception e) {
            LOG.error("Format to xml", e);
        }

//        return new SparkResultSet();
        return sb.toString();
    }

    public void updateRowcount(SparkResultSet rs) {
        try {
            updateSys(SystemVName.ROWCOUNT, null == rs ? 0 : rs.getRow());
        } catch (Exception e) {
            e.printStackTrace();
        }
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
