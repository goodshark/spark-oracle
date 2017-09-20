package org.apache.hive.tsql.common;

import org.apache.hive.plsql.cursor.OracleCursor;
import org.apache.hive.plsql.type.AssocArrayTypeDeclare;
import org.apache.hive.plsql.type.LocalTypeDeclare;
import org.apache.hive.plsql.type.VarrayTypeDeclare;
import org.apache.hive.tsql.arg.SystemVName;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.cfl.GotoStatement;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plfunc.PlFunctionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhongdg1 on 2016/11/29.
 */
public abstract class BaseStatement extends TreeNode {
    public static final String CODE_SEP = " ";
    public static final String CODE_EQ = "=";
    public static final String CODE_EQ2 = "==";
    public static final String CODE_END = ";";
    public static final String CODE_OR = "||";
    public static final String CODE_AND = "&&";
    public static final String CODE_NOT = "!";
    public static final String CODE_LINE_END = "\n";

    private static final Logger LOG = LoggerFactory.getLogger(BaseStatement.class);
    private StringBuffer exeSql = new StringBuffer();
    // store labels with while
    private Set<String> labels = new HashSet<>();
    // check labels belong to while is searched
    private boolean labelSearched = false;

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
        /*SparkResultSet sparkResultSet = new SparkResultSet();
        sparkResultSet.addColumn(new Column("id", ColumnDataType.INT));
        sparkResultSet.addColumn(new Column("name", ColumnDataType.STRING));
        sparkResultSet.addColumn(new Column("age", ColumnDataType.INT));
        for (int i = 0; i < 10; i++) {
            sparkResultSet.addRow(new Object[]{i * 3, "TEST_" + i, i * 11});
        }
        if (isAddResult()) {
            getExecSession().addRs(sparkResultSet);
        }
        return sparkResultSet;*/
        //For testing end
        SparkSession sparkSession = getExecSession().getSparkSession();
        LogicalPlan plan = sparkSession.sqlContext().sessionState().sqlParser().parsePlan(exeSql);
        getExecSession().addLogicalPlans(plan);
        Dataset dataset = sparkSession.sql(exeSql);
        SparkResultSet sparkResultSet = new SparkResultSet(dataset);
        updateRowcount(sparkResultSet);
        if (isAddResult()) {
            getExecSession().addRs(sparkResultSet);
        }

        return sparkResultSet;
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

    public void setLabelSearched(Set<String> labs) {
        labelSearched = true;
        labels.addAll(labs);
    }

    public boolean isLabelSearched() {
        return labelSearched;
    }

    public boolean existLabel(String name) {
        if (!isLabelSearched()) {
            Set<String> labs = searchAllLabels();
            setLabelSearched(labs);
        }
        return labels.contains(name.toLowerCase());
    }

    public Set<String> searchAllLabels() {
        Set<String> labelSet = new HashSet<>();
        TreeNode pNode = getParentNode();
        // a normal CFL-node in AST tree always has a parent node
        if (pNode == null) {
            return labelSet;
        }
        List<TreeNode> childList = pNode.getChildrenNodes();
        // label always before whileStmt
        for (TreeNode child : childList) {
            if (child.equals(this))
                break;
            else {
                if (child.getNodeType() == TreeNode.Type.GOTO) {
                    GotoStatement gotoStatement = (GotoStatement) child;
                    if (!gotoStatement.getAction()) {
                        labelSet.add(gotoStatement.getLabel());
                    }
                }
            }
        }
        return labelSet;
    }

    private void subsititueType(Var curVar, Var refVar) {
    }

    protected void resolveRefSingle(Var var) throws Exception {
        String refTypeName = var.getRefTypeName();
        Var refVar = findVar(refTypeName);
        if (refVar == null) {
            // refType reference table column
            String[] strs = refTypeName.split("\\.");
            if (strs.length < 2)
                throw new Exception("REF_SINGLE %Type is unknown Type: " + var.getRefTypeName());
            String tblName = strs[0];
            String colName = strs[1];
            Dataset<org.apache.spark.sql.catalog.Column> cols = getExecSession().getSparkSession().catalog().listColumns(tblName);
            org.apache.spark.sql.catalog.Column[] columns = (org.apache.spark.sql.catalog.Column[]) cols.collect();
            for (org.apache.spark.sql.catalog.Column col: columns) {
                if (col.name().equalsIgnoreCase(colName)) {
                    var.setDataType(Var.DataType.valueOf(col.dataType().toUpperCase().replaceAll("\\(.*\\)", "")));
                    break;
                }
            }
        } else {
            // refType reference pre-exists var Type
            var.setDataType(refVar.getDataType());
            // TODO data type is complex, need add inner var
            subsititueType(var, refVar);
        }
    }

    protected void resolveRefComposite(Var var) throws Exception {
        String complexRefName= var.getRefTypeName();
        OracleCursor cursor = (OracleCursor) findCursor(complexRefName);
        if (cursor == null) {
            // reference table
            String tblName = complexRefName;
            Dataset<org.apache.spark.sql.catalog.Column> cols = getExecSession().getSparkSession().catalog().listColumns(tblName);
            org.apache.spark.sql.catalog.Column[] columns = (org.apache.spark.sql.catalog.Column[]) cols.collect();
            for (org.apache.spark.sql.catalog.Column col: columns) {
                Var innerVar = new Var();
                String colVarName = col.name();
                Var.DataType colDataType = Var.DataType.valueOf(col.dataType().toUpperCase().replaceAll("\\(.*\\)", ""));
                innerVar.setDataType(colDataType);
                innerVar.setVarName(colVarName);
                var.addInnerVar(innerVar);
            }
            var.setCompoundResolved();
        } else {
            // cursor ref complex Type will postpone Type inference after open cursor
        }
    }

    protected void resolveCustomType(Var var) throws Exception {
        LocalTypeDeclare typeDeclare = findType(var.getRefTypeName());
        if (typeDeclare == null)
            throw new Exception("type " + var.getRefTypeName() + " not exist");
        if (typeDeclare.getDeclareType() == Var.DataType.COMPOSITE) {
            var.setDataType(Var.DataType.REF_COMPOSITE);
            if (typeDeclare.isResolved())
                var.setCompoundResolved();
            Map<String, Var> typeVars = typeDeclare.getTypeVars();
            for (String fieldName: typeVars.keySet()) {
                Var typeVar = typeVars.get(fieldName);
                var.addInnerVar(typeVar.typeClone());
            }
        } else if (typeDeclare.getDeclareType() == Var.DataType.VARRAY) {
            var.setDataType(Var.DataType.VARRAY);
            var.addVarrayTypeVar(((VarrayTypeDeclare)typeDeclare).getTypeVar());
        } else if (typeDeclare.getDeclareType() == Var.DataType.NESTED_TABLE) {
            var.setDataType(Var.DataType.NESTED_TABLE);
            var.addNestedTableTypeVar(typeDeclare.getTableTypeVar());
            // TODO compatible old ARRAY
            var.addArrayVar(typeDeclare.getTableTypeVar().typeClone());
        } else if (typeDeclare.getDeclareType() == Var.DataType.ASSOC_ARRAY) {
            var.setDataType(Var.DataType.ASSOC_ARRAY);
            var.setAssocTypeVar(((AssocArrayTypeDeclare)typeDeclare).getIndexTypeVar());
        } else {
            var.setDataType(Var.DataType.NESTED_TABLE);
            Var typeVar = typeDeclare.getTableTypeVar();
            var.addArrayVar(typeVar.typeClone());
        }
    }
    public String doCodegen(List<String> variables, List<String> childPlfuncs, PlFunctionRegistry.PlFunctionIdentify current, String returnType) throws Exception{throw new Exception("Base not support codegen");};
}
