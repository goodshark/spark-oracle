package org.apache.hive.plsql.function;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hive.basesql.func.CallStatement;
import org.apache.hive.basesql.func.CommonProcedureStatement;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.ddl.CreateFunctionStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;
import org.apache.hive.tsql.exception.NotDeclaredException;
import org.apache.hive.tsql.execute.Executor;
import org.apache.hive.tsql.func.FuncName;
import org.apache.hive.tsql.func.Procedure;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.FunctionIdentifier;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.catalyst.plfunc.PlFunctionRegistry;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import scala.collection.JavaConversions;

import java.text.ParseException;
import java.util.*;

/**
 * Created by dengrb1 on 5/24 0024.
 */

public class ProcedureCall extends CallStatement {
    private static final String STATEMENT_NAME = "_ORACLE_PROC_CALL_";
    private SparkSession sparkSession;

    public ProcedureCall(FuncName name) {
        super(STATEMENT_NAME);
        setFuncName(name);
        setRealFuncName(name.getFullFuncName());
    }

    public void setSparkSession(SparkSession sparkSession){
        this.sparkSession = sparkSession;
    }

    @Override
    public void call() throws Exception {
        // replace formal parameter with actual parameter
        Map<String, String> assigned = new HashMap<>();

        for (int i = 0; i < arguments.size(); i++) {
            Var argument = this.arguments.get(i);
            String argName = argument.getVarName();
            Var funcVar = null;
            funcVar = argName == null ? func.getInAndOutputs().get(i).clone() : findInputVar(argName);
            if (null == funcVar) {
                throw new NotDeclaredException(argName);
            }
            assignToFunc(argument, funcVar);
            assigned.put(funcVar.getVarName(), funcVar.getVarName());
            addVar(funcVar);
        }
        addUnassignArguments(func, assigned);
        // execute
        int rsCount = getExecSession().getResultSets().size();
        new Executor(getExecSession(), func.getSqlClauses()).run();
    }

    @Override
    protected void postExecute() throws Exception {
        // update variables if necessary (OUT/IN OUT)
        List<Var> newOutputs = new ArrayList<>();
        for (Var arg: func.getInAndOutputs()) {
            if (arg.getVarType() == Var.VarType.OUTPUT || arg.getVarType() == Var.VarType.INOUT) {
                Var outArg = findVar(arg.getVarName());
                if (arg.getMapOutName() != null)
                    outArg.setVarName(arg.getMapOutName());
                if (outArg != null)
                    newOutputs.add(outArg);
            }
        }
        super.postExecute();
        for (Var v : newOutputs) {
            addVar(v);
        }

        Var returnVar = getExecSession().getVariableContainer().getFuncReturnVar();
        if (returnVar != null) {
            setRs(new SparkResultSet().addRow(new Object[] {returnVar}));
            getExecSession().getVariableContainer().clearFuncReturnVar();
        }
        /*if (null != returnVar) {
            addVar(returnVar);
        }
        int newRsCount = getExecSession().getResultSets().size();
        if (!isCollectRs() && newRsCount > rsCount) {
            this.setRs(getExecSession().getResultSets().remove(newRsCount - 1));
        }*/
    }


    private Var findInputVar(String name) {
        for (Var var : this.func.getInAndOutputs()) {
            if (var.getVarName().equals(name)) {
                return var.clone();
            }
        }
        return null;
    }

    private Object getValueFromVar(Var var) throws Exception {
        if (var.getValueType() == Var.ValueType.EXPRESSION) {
            TreeNode base = var.getExpr();
            // compatible with sqlserver
            if (base == null) {
                Var realVar = findVar(var.getVarValue().toString());
                if (realVar == null)
                    throw new NotDeclaredException(var.getVarValue().toString());
                return realVar.getVarValue();
            }
            base.setExecSession(getExecSession());
            base.execute();
            Var baseVar = (Var) base.getRs().getObject(0);
            return baseVar.getVarValue();
        } else {
            return var.getVarValue();
        }
    }

    private void assignToFunc(Var argument, Var funcVar) throws Exception {
        Object val = getValueFromVar(argument);
        funcVar.setVarValue(val);
        // config proc/func output map name
        if (funcVar.getVarType() == Var.VarType.OUTPUT || funcVar.getVarType() == Var.VarType.INOUT) {
            ExpressionStatement es = (ExpressionStatement) argument.getExpr();
            if (es != null) {
                String outName = es.getExpressionBean().getVar().getVarName();
                for (Var funcArg: func.getInAndOutputs()) {
                    if (funcArg.getVarName().equalsIgnoreCase(funcVar.getVarName()))
                        funcArg.setMapOutName(outName);
                }
            }
            if (funcVar.getVarType() == Var.VarType.OUTPUT)
                funcVar.setVarValue(null);
        }
        funcVar.setExecuted(true);
    }

    private void addUnassignArguments(CommonProcedureStatement function, Map<String, String> assigned) {
        for (Var vv : function.getInAndOutputs()) {
            if (null != assigned.get(vv.getVarName().toUpperCase())) {
                continue;
            }
            addVar(vv.clone());
        }
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public String doCodegen(List<String> variables, List<String> childPlfuncs) throws Exception{
        StringBuffer sb = new StringBuffer();
        String functionName = null;
        String functionDb = null;
        if(funcName.getDatabase() != null){
            functionDb = funcName.getDatabase();
            functionName = funcName.getFuncName();
        } else {
            if(funcName.getFuncName().contains(".")){
                String[] strs = funcName.getFuncName().split("\\.");
                if(strs.length == 2){
                    functionDb = strs[0];
                    functionName = strs[1];
                }
            } else {
                functionName = funcName.getFuncName();
                functionDb = sparkSession.getSessionState().catalog().getCurrentDatabase();
            }
        }
        if(functionDb == null || functionName == null){
            throw new Exception("Can not obtain DATABASE or FUNCTIONNAME.");
        }
        PlFunctionRegistry.PlFunctionDescription f = PlFunctionRegistry.getInstance().getPlFunc(new PlFunctionRegistry.
                PlFunctionIdentify(functionName, functionDb));
        if(f != null){
            sb.append("(");
            sb.append(f.getReturnType());
            sb.append(")");
            StringBuilder declare = new StringBuilder();
            String cname = functionDb + "_" + functionName;
            String funcVar = "v" + cname + arguments.size();
            declare.append(cname);
            declare.append(CODE_SEP);
            declare.append(funcVar);
            declare.append(CODE_EQ);
            declare.append("new ");
            declare.append(cname);
            declare.append("()");
            if(!variables.contains(declare.toString())){
                variables.add(declare.toString());
            }
            StringBuilder args = new StringBuilder();
            int j = 0;
            for(Var arg : arguments){
                if (arg.getDataType() == Var.DataType.VAR) {
                    Object value = null;
                    try{
                        value = arg.getVarValue();
                    } catch (ParseException e) {
                        //TODO
                    }
                    if(value != null){
                        args.append("((Object)");
                        args.append("\"");
                        args.append(value.toString());
                        args.append("\"");
                        args.append(")");
                    } else {
                        args.append(arg.getVarName() + " == null ? null : ((Object)");
                        args.append(arg.getVarName());
                        args.append(")");
                    }
                } else  if (arg.getValueType() == Var.ValueType.EXPRESSION) {
                    TreeNode baseStatement = arg.getExpr();
                    if(baseStatement instanceof BaseStatement){
                        String code = ((BaseStatement) baseStatement).doCodegen(variables, childPlfuncs);
                        args.append(code + " == null ? null : ((Object)");
                        args.append(code);
                        args.append(")");
                    }
                } else {
                    args.append("((Object)");
                    args.append("\"");
                    args.append(arg.toString());
                    args.append("\"");
                    args.append(")");
                }
                j++;
                if(j != arguments.size()){
                    sb.append(",");
                }
            }
            sb.append(funcVar + ".eval(new Object[]{" + args.toString() + "})");
            childPlfuncs.add(functionDb + "." + functionName);
        } else {
            ExpressionInfo expressionInfo = sparkSession.getSessionState().catalog().lookupFunctionInfo(new FunctionIdentifier(funcName.getFuncName()));
            String classname = expressionInfo.getClassName();
            if(classname == null || "".equals(classname)){
                throw new Exception("Can not obtain Function class for [" + funcName.getFuncName() + "]");
            } else {
                String[] sim = classname.split("\\.");
                String simpleName = sim[sim.length-1];
                String funcVar = "v" + simpleName.toLowerCase() + arguments.size();
                StringBuilder declare = new StringBuilder();
                declare.append(classname);
                declare.append(CODE_SEP);
                declare.append(funcVar);
                declare.append(CODE_EQ);
                declare.append("new ");
                declare.append(classname);

                if(isExtendFrom(classname, UDF.class)){
                    declare.append("()");
                    if(!variables.contains(declare.toString())){
                        variables.add(declare.toString());
                    }
                    sb.append(funcVar + ".evaluate(");
                    int j = 0;
                    for(Var arg : arguments){
                        if (arg.getDataType() == Var.DataType.VAR) {
                            Object value = null;
                            try{
                                value = arg.getVarValue();
                            } catch (ParseException e) {
                                //TODO
                            }
                            if(value != null){
                                sb.append("\"");
                                sb.append(value.toString());
                                sb.append("\"");
                            } else {
                                sb.append(arg.getVarName());
                            }
                        } else  if (arg.getValueType() == Var.ValueType.EXPRESSION) {
                            TreeNode baseStatement = arg.getExpr();
                            if(baseStatement instanceof BaseStatement){
                                String code = ((BaseStatement) baseStatement).doCodegen(variables, childPlfuncs);
                                sb.append(code);
                            }
                        } else {
                            sb.append("\"");
                            sb.append(arg.toString());
                            sb.append("\"");
                        }
                        j++;
                        if(j != arguments.size()){
                            sb.append(",");
                        }
                    }
                }

                if(isExtendFrom(classname, Expression.class) && !isExtendFrom(classname, UnaryExpression.class)){
                    declare.append("(scala.collection.JavaConversions.asScalaBuffer(java.util.Arrays.asList(new org.apache.spark.sql.catalyst.expressions.Expression[]{");
                    int i = 0;
                    while (i < arguments.size()) {
                        declare.append("new org.apache.spark.sql.catalyst.expressions.FunctionValueExpression(" + i + ")");
                        i++;
                        if(i != arguments.size()){
                            declare.append(",");
                        }
                    }
                    declare.append("})))");
                    if(!variables.contains(declare.toString())){
                        variables.add(declare.toString());
                    }

                    String rowVarName = this.realFuncName + "row" + arguments.size();
                    StringBuilder rowDeclare = new StringBuilder();
                    rowDeclare.append("org.apache.spark.sql.catalyst.expressions.FunctionArgsRow" + CODE_SEP + rowVarName + CODE_EQ + "new org.apache.spark.sql.catalyst.expressions.FunctionArgsRow(" + arguments.size() +")");
                    if(!variables.contains(rowDeclare.toString())){
                        variables.add(rowDeclare.toString());
                    }
                    sb.append(funcVar);
                    sb.append(".eval(");
                    sb.append(rowVarName);
                    sb.append(".update(new org.apache.spark.sql.catalyst.expressions.UpdateValue[]{");
                    int j = 0;
                    for(Var arg : arguments){
                        if (arg.getDataType() == Var.DataType.VAR) {
                            Object value = null;
                            try{
                                value = arg.getVarValue();
                            } catch (ParseException e) {
                                //TODO
                            }
                            if(value != null){
                                sb.append("new org.apache.spark.sql.catalyst.expressions.UpdateValue(");
                                sb.append(j);
                                sb.append(",\"");
                                sb.append(value.toString());
                                sb.append("\")");
                            } else {
                                sb.append("new org.apache.spark.sql.catalyst.expressions.UpdateValue(");
                                sb.append(j);
                                sb.append(",");
                                sb.append(arg.getVarName());
                                sb.append(")");
                            }
                        } else  if (arg.getValueType() == Var.ValueType.EXPRESSION) {
                            TreeNode baseStatement = arg.getExpr();
                            if(baseStatement instanceof BaseStatement){
                                String code = ((BaseStatement) baseStatement).doCodegen(variables, childPlfuncs);
                                sb.append("new org.apache.spark.sql.catalyst.expressions.UpdateValue(");
                                sb.append(j);
                                sb.append(",");
                                sb.append(code);
                                sb.append(")");
                            }
                        } else {
                            sb.append("new org.apache.spark.sql.catalyst.expressions.UpdateValue(");
                            sb.append(j);
                            sb.append(",\"");
                            sb.append(arg.toString());
                            sb.append("\")");
                        }
                        j++;
                        if(j != arguments.size()){
                            sb.append(",");
                        }
                    }
                    sb.append("}))");
                }
                if(isExtendFrom(classname, UnaryExpression.class) && arguments.size() == 1){

                    declare.append("(new new org.apache.spark.sql.catalyst.expressions.FunctionValueExpression(0))");
                    if(!variables.contains(declare.toString())){
                        variables.add(declare.toString());
                    }

                    String rowVarName = this.realFuncName + "row" + arguments.size();
                    StringBuilder rowDeclare = new StringBuilder();
                    rowDeclare.append("org.apache.spark.sql.catalyst.expressions.FunctionArgsRow" + CODE_SEP + rowVarName + CODE_EQ + "new org.apache.spark.sql.catalyst.expressions.FunctionArgsRow(" + arguments.size() +")");
                    if(!variables.contains(rowDeclare.toString())){
                        variables.add(rowDeclare.toString());
                    }
                    sb.append(funcVar);
                    sb.append(".eval(row.update(new org.apache.spark.sql.catalyst.expressions.UpdateValue[]{");
                    Var arg = arguments.get(0);
                    if (arg.getDataType() == Var.DataType.VAR) {
                        Object value = null;
                        try{
                            value = arg.getVarValue();
                        } catch (ParseException e) {
                        }
                        if(value != null){
                            sb.append("new org.apache.spark.sql.catalyst.expressions.UpdateValue(");
                            sb.append(0);
                            sb.append(",\"");
                            sb.append(value.toString());
                            sb.append("\")");
                        } else {
                            sb.append("new org.apache.spark.sql.catalyst.expressions.UpdateValue(");
                            sb.append(0);
                            sb.append(",");
                            sb.append(arg.getVarName());
                            sb.append(")");
                        }
                    } else  if (arg.getValueType() == Var.ValueType.EXPRESSION) {
                        TreeNode baseStatement = arg.getExpr();
                        if(baseStatement instanceof BaseStatement){
                            String code = ((BaseStatement) baseStatement).doCodegen(variables, childPlfuncs);
                            sb.append("new org.apache.spark.sql.catalyst.expressions.UpdateValue(");
                            sb.append(0);
                            sb.append(",");
                            sb.append(code);
                            sb.append(")");
                        }
                    } else {
                        sb.append("new org.apache.spark.sql.catalyst.expressions.UpdateValue(");
                        sb.append(0);
                        sb.append(",\"");
                        sb.append(arg.toString());
                        sb.append("\")");
                    }
                    sb.append("}))");
                }
            }
        }
        return sb.toString();
    }

    private String getDataType(Var args) {
        if("EXPRESSION".equalsIgnoreCase(args.getDataType().name())){

        }
        return null;
    }

    private void getValueVars(ExpressionStatement express , List<Var> list) {
        if(express.getChildrenNodes().size() == 0 && express.getExpressionBean() != null && express.getExpressionBean().getVar() != null){
            list.add(express.getExpressionBean().getVar());
        }

    }

    private String getDataType(String expr, List<Var> knowns) {
        String dt = null;
        for(Var var : knowns){
            if(expr.contains(var.getVarName())){
                dt = var.getDataType().name();
            }
        }
        if(dt != null){
            CreateFunctionStatement.SupportDataTypes type = CreateFunctionStatement.fromString(dt);
            switch(type) {
                case INT:return "INTEGER";
                case LONG:return "LONG";
                case FLOAT:return "FLOAT";
                case DOUBLE:return "DOUBLE";
                case STRING:return "STRING";
                case BOOLEAN:return "BOOLEAN";
                case INTEGER:return "INTEGER";
                case CHAR:return "STRING";
                case VARCHAR:return "STRING";
                case VARCHAR2:return "STRING";
                default:return null;
            }
        } else {
            return null;
        }
    }

    private Boolean isExtendFrom(String className, Class ex){
        try{
            Class c = Class.forName(className);
            return ex.isAssignableFrom(c);
        } catch (ClassNotFoundException e) {
            //TODO
        }
        return  false;
    }
}
