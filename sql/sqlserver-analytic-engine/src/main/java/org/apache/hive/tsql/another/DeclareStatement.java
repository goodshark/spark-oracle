package org.apache.hive.tsql.another;

import org.apache.hive.plsql.cursor.OracleCursor;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TmpTableNameUtils;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.ddl.CreateFunctionStatement;
import org.apache.spark.sql.catalog.Column;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.Dataset;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhongdg1 on 2016/12/1.
 */
public class DeclareStatement extends BaseStatement {
    private static final String STATEMENT_NAME = "_DECLARE_";
    private List<Var> declareVars = new ArrayList<>();

    public DeclareStatement() {
        super(STATEMENT_NAME);
    }

    public void addDeclareVar(Var var) {
        this.declareVars.add(var);
    }

    @Override
    public int execute() throws Exception {
        for (Var var : declareVars) {
//            if (null != findVar(var.getVarName())) {
//                throw new AlreadyDeclaredException(var.getVarName());
//            }
            if (var.getDataType() == Var.DataType.REF) {
                resolveRefVar(var);
//                findRefType(var);
                // REF has no default value
//                continue;
            }
            if (var.getDataType() == Var.DataType.COMPLEX) {
                resolveComplexVar(var);
//                findComplexType(var);
                // COMPLEX has no default value
//                continue;
            }
            if (var.getDataType() == Var.DataType.CUSTOM) {
                resolveCustomType(var);
            }

            switch (var.getValueType()) {
                case TABLE:
                    TmpTableNameUtils tableNameUtils =new  TmpTableNameUtils();
                    String aliasName = tableNameUtils.createTableName(var.getVarName());
                    StringBuffer sb = new StringBuffer();
                    sb.append("CREATE TABLE ").append(aliasName).append("(").
                            append(var.getVarValue().toString()).append(")");
                    //TODO EXECUTE CREATE TABLE IN SPARK
                    commitStatement(sb.toString());
                    Var tVar = new Var(var.getVarName(), var.getVarValue(), Var.DataType.TABLE);
                    tVar.setAliasName(aliasName);
                    addTableVars(tVar);
                    break;
                case EXPRESSION:
                    //1. 计算表达式的值 2. 将变量加入到容器
                    TreeNode statement = var.getExpr();
                    statement.setExecSession(getExecSession());
                    if (null == statement) {
                        break;
                    }
                    statement.execute();
                    var.setVarValue(((Var) statement.getRs().getObject(0)).getVarValue());
                    var.setExecuted(true);
                    break;
                default:
                    break;
            }
            addVar(var);
        }


        return 0;
    }

    /*private void findRefType(Var var) throws Exception {
        String refTypeName = var.getRefTypeName();
        Var refVar = findVar(refTypeName);
        if (refVar == null) {
            // refType reference table column
            String[] strs = refTypeName.split("\\.");
            if (strs.length < 2)
                throw new Exception("REF %Type is unknown Type: " + var.getRefTypeName());
            String tblName = strs[0];
            String colName = strs[1];
            Dataset<Column> cols = getExecSession().getSparkSession().catalog().listColumns(tblName);
            Column[] columns = (Column[]) cols.collect();
            for (Column col: columns) {
                if (col.name().equalsIgnoreCase(colName)) {
                    var.setDataType(Var.DataType.valueOf(col.dataType().toUpperCase().replaceAll("\\(.*\\)", "")));
                    break;
                }
            }
        } else {
            // refType reference pre-exists var Type
            var.setDataType(refVar.getDataType());
        }
    }*/

    /*private void findComplexType(Var var) throws Exception {
        String complexRefName= var.getRefTypeName();
        OracleCursor cursor = (OracleCursor) findCursor(complexRefName);
        if (cursor == null) {
            // reference table
            String tblName = complexRefName;
            Dataset<Column> cols = getExecSession().getSparkSession().catalog().listColumns(tblName);
            Column[] columns = (Column[]) cols.collect();
            for (Column col: columns) {
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
    }*/

    @Override
    public BaseStatement createStatement() {
        return this;
    }

    @Override
    public String doCodegen(){
        StringBuffer sb = new StringBuffer();
        String varName = declareVars.get(0).getVarName();
        CreateFunctionStatement.SupportDataTypes dataType = CreateFunctionStatement.fromString(declareVars.get(0).getDataType().name());
        sb.append(dataType.toString());
        sb.append(BaseStatement.CODE_SEP);
        sb.append(varName);
        if("EXPRESSION".equalsIgnoreCase(declareVars.get(0).getValueType().name()) && declareVars.get(0).getExpr() != null){
            if(declareVars.get(0).getExpr() instanceof BaseStatement){
                sb.append(CODE_EQ);
                BaseStatement bs = (BaseStatement)declareVars.get(0).getExpr();
                sb.append(bs.doCodegen());
            }
        }
        sb.append(CODE_END);
        return sb.toString();
    }
}
