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
import org.apache.spark.sql.types.*;
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
        setRealFuncName(name.getRealFullFuncName());
        type = 2;
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

    /*private Object getValueFromVar(Var var) throws Exception {
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
    }*/

    /*private Var getVarFromArg(Var argVar) throws Exception {
        if (argVar.getValueType() == Var.ValueType.EXPRESSION) {
            TreeNode base = argVar.getExpr();
            // compatible with sqlserver
            if (base == null) {
                Var realVar = findVar(argVar.getVarValue().toString());
                if (realVar == null)
                    throw new NotDeclaredException(argVar.getVarValue().toString());
                return realVar;
            }
            base.setExecSession(getExecSession());
            base.execute();
            Var baseVar = (Var) base.getRs().getObject(0);
            return baseVar;
        } else {
            return argVar;
        }
    }*/

    private void assignToFunc(Var argument, Var funcVar) throws Exception {
//        Object val = getValueFromVar(argument);
//        funcVar.setVarValue(val);
        // support collection-type argument
        Var argVar = getVarFromArg(argument);
        funcVar.setDataType(argVar.getDataType());
        Var.assign(funcVar, argVar);
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
    public String doCodegen(List<String> variables, List<String> childPlfuncs, PlFunctionRegistry.PlFunctionIdentify current, String returnType) throws Exception{
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
        String enname = sparkSession.sessionState().conf().getConfString("spark.sql.analytical.engine", "spark");
        if(current.equals(new PlFunctionRegistry.PlFunctionIdentify(functionName, functionDb))){
            sb.append("(");
            sb.append(returnType);
            sb.append(")");
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
                        args.append("(Object)(");
                        args.append(arg.getVarName() + ") == null ? null : ((Object)(");
                        args.append(arg.getVarName());
                        args.append("))");
                    }
                } else  if (arg.getValueType() == Var.ValueType.EXPRESSION) {
                    TreeNode baseStatement = arg.getExpr();
                    if(baseStatement instanceof BaseStatement){
                        String code = ((BaseStatement) baseStatement).doCodegen(variables, childPlfuncs, current, returnType);
                        args.append("(Object)(");
                        args.append(code + ") == null ? null : ((Object)(");
                        args.append(code);
                        args.append("))");
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
                    args.append(",");
                }
            }
            sb.append("eval(new Object[]{" + args.toString() + "})");
        } else {
            PlFunctionRegistry.PlFunctionDescription f = null;
            if("oracle".equalsIgnoreCase(enname)){
                f= PlFunctionRegistry.getInstance().getOraclePlFunc(new PlFunctionRegistry.
                        PlFunctionIdentify(functionName, functionDb));
            }
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
                            args.append("(Object)(");
                            args.append(arg.getVarName() + ") == null ? null : ((Object)(");
                            args.append(arg.getVarName());
                            args.append("))");
                        }
                    } else  if (arg.getValueType() == Var.ValueType.EXPRESSION) {
                        TreeNode baseStatement = arg.getExpr();
                        if(baseStatement instanceof BaseStatement){
                            String code = ((BaseStatement) baseStatement).doCodegen(variables, childPlfuncs, current, returnType);
                            args.append("(Object)(");
                            args.append(code + ") == null ? null : ((Object)(");
                            args.append(code);
                            args.append("))");
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
                        args.append(",");
                    }
                }
                sb.append(funcVar + ".eval(new Object[]{" + args.toString() + "})");
                childPlfuncs.add(functionDb + "." + functionName);
            } else {
                ExpressionInfo expressionInfo = sparkSession.getSessionState().catalog().lookupFunctionInfo(new FunctionIdentifier(funcName.getFuncName()));
                String classname = expressionInfo.getClassName();
                if(classname == null || "".equals(classname) || sparkSession.getSessionState().catalog().lookupFunctionBuilder(new FunctionIdentifier(funcName.getFuncName())) == null){
                    throw new Exception("Can not obtain Function Info for [" + funcName.getFuncName() + "]");
                } else {
                    String[] sim = classname.split("\\.");
                    String simpleName = sim[sim.length-1];
                    String funcVar = "v" + simpleName.toLowerCase() + arguments.size();
                    StringBuilder declare = new StringBuilder();
                    declare.append(classname);
                    declare.append(CODE_SEP);
                    declare.append(funcVar);
                    declare.append(CODE_EQ);


                    if(isExtendFrom(classname, UDF.class)){
                        declare.append("new ");
                        declare.append(classname);
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
                                    String code = ((BaseStatement) baseStatement).doCodegen(variables, childPlfuncs, current, returnType);
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

                    if(isExtendFrom(classname, Expression.class)){
                        declare.append("(");
                        declare.append(classname);
                        declare.append(")");
                        declare.append("org.apache.spark.sql.catalyst.plfunc.PlFunctionUtils.getExpressionInstance(\"");
                        declare.append(classname);
                        declare.append("\",");
                        declare.append(arguments.size());
                        declare.append(")");
                        if(!variables.contains(declare.toString())){
                            variables.add(declare.toString());
                        }

                        List<Expression> expressions = new ArrayList<>(arguments.size());
                        for(int i = 0; i < arguments.size(); i++){
                            expressions.add(new FunctionValueExpression(i));
                        }
                        Expression expr = sparkSession.getSessionState().catalog().lookupFunctionBuilder(new FunctionIdentifier(funcName.getFuncName())).get().apply(scala.collection.JavaConversions.asScalaBuffer(expressions));
                        DataType functionType = expr.dataType();
                        List<AbstractDataType> inputTypes = null;
                        if(expr instanceof ImplicitCastInputTypes){
                            inputTypes = scala.collection.JavaConversions.bufferAsJavaList(((ImplicitCastInputTypes) expr).inputTypes().<AbstractDataType>toBuffer());
                        }
                        String functionReturnType = null;
                        if(functionType instanceof StringType){
                            functionReturnType = "String";
                        } else if(functionType instanceof LongType){
                            functionReturnType = "Long";
                        } else if(functionType instanceof DoubleType){
                            functionReturnType = "Double";
                        } else if(functionType instanceof FloatType){
                            functionReturnType = "Float";
                        } else if(functionType instanceof BooleanType){
                            functionReturnType = "Boolean";
                        } else if(functionType instanceof IntegerType){
                            functionReturnType = "Integer";
                        } else {
                            throw new Exception("Not support for type = " + functionType.toString());
                        }
                        sb.append("(");
                        sb.append(functionReturnType);
                        sb.append(")");

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
                            boolean stringt = false;
                            if(inputTypes != null && inputTypes.get(j) instanceof StringType){
                                stringt = true;
                            }
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
                                    sb.append(",");
                                    if(stringt){
                                        sb.append("UTF8String.fromString");
                                    }
                                    sb.append("(\"");
                                    sb.append(value.toString());
                                    sb.append("\"))");
                                } else {
                                    sb.append("new org.apache.spark.sql.catalyst.expressions.UpdateValue(");
                                    sb.append(j);
                                    sb.append(",");
                                    if(stringt){
                                        sb.append("UTF8String.fromString");
                                    }
                                    sb.append("(");
                                    sb.append(arg.getVarName());
                                    sb.append("))");
                                }
                            } else  if (arg.getValueType() == Var.ValueType.EXPRESSION) {
                                TreeNode baseStatement = arg.getExpr();
                                if(baseStatement instanceof BaseStatement){
                                    String code = ((BaseStatement) baseStatement).doCodegen(variables, childPlfuncs, current, returnType);
                                    sb.append("new org.apache.spark.sql.catalyst.expressions.UpdateValue(");
                                    sb.append(j);
                                    sb.append(",");
                                    if(stringt){
                                        sb.append("UTF8String.fromString");
                                    }
                                    sb.append("(");
                                    sb.append(code);
                                    sb.append("))");
                                }
                            } else {
                                sb.append("new org.apache.spark.sql.catalyst.expressions.UpdateValue(");
                                sb.append(j);
                                sb.append(",");
                                if(stringt){
                                    sb.append("UTF8String.fromString");
                                }
                                sb.append("(\"");
                                sb.append(arg.toString());
                                sb.append("\"))");
                            }
                            j++;
                            if(j != arguments.size()){
                                sb.append(",");
                            }
                        }
                        sb.append("}))");
                        if("String".equalsIgnoreCase(functionReturnType)) {
                            sb.append(".toString()");
                        }
                    }
                }
            }
        }
        return sb.toString();
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

    public String getOriginalSql() {
        StringBuilder sb = new StringBuilder(getRealFuncName());
        sb.append("(");
        boolean flag = false;
        for(Var arg: arguments) {
            if(flag) {
                sb.append(",");
            }
            sb.append(arg.getOriginalSql());
            flag = true;
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String getFinalSql() throws Exception {
        return getOriginalSql();
    }
}
