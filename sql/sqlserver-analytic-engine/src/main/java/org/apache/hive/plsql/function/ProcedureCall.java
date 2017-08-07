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
import org.apache.spark.sql.catalyst.expressions.*;
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
    private ExpressionInfo funcclass;

    public ProcedureCall(FuncName name) {
        super(STATEMENT_NAME);
        setFuncName(name);
        setRealFuncName(name.getFullFuncName());
    }

    public void setFuncclass(ExpressionInfo funcclass){
        this.funcclass = funcclass;
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
    public String doCodegen(List<String> imports, List<String> variables, List<Var> knownVars){
        StringBuffer sb = new StringBuffer();
        String classname = funcclass.getClassName();
        if(!imports.contains(classname)) {
            imports.add(classname);
        }
        String[] sim = classname.split(".");
        String simpleName = sim[sim.length-1];
        String funcVar = "v" + simpleName.toLowerCase() + arguments.size();
        StringBuilder declare = new StringBuilder();
        declare.append(simpleName);
        declare.append(CODE_SEP);
        declare.append(funcVar);
        declare.append(CODE_EQ);
        declare.append("new ");
        declare.append(simpleName);

        if(isExtendFrom(classname, UDF.class)){
            declare.append("()");
            if(!variables.contains(declare.toString())){
                variables.add(declare.toString());
            }
            sb.append(funcVar + "(");
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
                        String code = ((BaseStatement) baseStatement).doCodegen(imports, variables, knownVars);
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
            if(!imports.contains(Expression.class.getName())){
                imports.add(Expression.class.getName());
            }
            if(!imports.contains(UpdateValue.class.getName())){
                imports.add(UpdateValue.class.getName());
            }
            if(!imports.contains(FunctionValueExpression.class.getName())){
                imports.add(FunctionValueExpression.class.getName());
            }
            if(!imports.contains(Arrays.class.getName())){
                imports.add(Arrays.class.getName());
            }
            if(!imports.contains(JavaConversions.class.getName())){
                imports.add(JavaConversions.class.getName());
            }

            declare.append("(JavaConversions.asScalaBuffer(Arrays.asList(new Expression[]{");
            int i = 0;
            while (i < arguments.size()) {
                declare.append("new FunctionValueExpression(" + i + ")");
                i++;
                if(i != arguments.size()){
                    declare.append(",");
                }
            }
            declare.append("})))");
            if(!variables.contains(declare.toString())){
                variables.add(declare.toString());
            }

            String rclass = FunctionArgsRow.class.getName();
            String rsclass = FunctionArgsRow.class.getSimpleName();
            if(!imports.contains(rclass)){
                imports.add(rclass);
            }
            if(!imports.contains(UpdateValue.class.getName())){
                imports.add(UpdateValue.class.getName());
            }
            String rowVarName = this.realFuncName + "row" + arguments.size();
            StringBuilder rowDeclare = new StringBuilder();
            rowDeclare.append(rsclass + CODE_SEP + rowVarName + CODE_EQ + "new FunctionArgsRow(" + arguments.size() +")");
            if(!variables.contains(rowDeclare.toString())){
                variables.add(rowDeclare.toString());
            }
            sb.append(funcVar);
            sb.append(".eval(row.update(new UpdateValue[]{");
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
                        sb.append("new UpdateValue(");
                        sb.append(j);
                        sb.append(",\"");
                        sb.append(value.toString());
                        sb.append("\")");
                    } else {
                        sb.append("new UpdateValue(");
                        sb.append(j);
                        sb.append(",");
                        sb.append(arg.getVarName());
                        sb.append(")");
                    }
                } else  if (arg.getValueType() == Var.ValueType.EXPRESSION) {
                    TreeNode baseStatement = arg.getExpr();
                    if(baseStatement instanceof BaseStatement){
                        String code = ((BaseStatement) baseStatement).doCodegen(imports, variables, knownVars);
                        sb.append("new UpdateValue(");
                        sb.append(j);
                        sb.append(",");
                        sb.append(code);
                        sb.append(")");
                    }
                } else {
                    sb.append("new UpdateValue(");
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
            if(!imports.contains(Expression.class.getName())){
                imports.add(Expression.class.getName());
            }
            if(!imports.contains(UpdateValue.class.getName())){
                imports.add(UpdateValue.class.getName());
            }
            if(!imports.contains(FunctionValueExpression.class.getName())){
                imports.add(FunctionValueExpression.class.getName());
            }
            if(!imports.contains(Arrays.class.getName())){
                imports.add(Arrays.class.getName());
            }
            if(!imports.contains(JavaConversions.class.getName())){
                imports.add(JavaConversions.class.getName());
            }

            declare.append("(new FunctionValueExpression(0))");
            if(!variables.contains(declare.toString())){
                variables.add(declare.toString());
            }

            String rclass = FunctionArgsRow.class.getName();
            String rsclass = FunctionArgsRow.class.getSimpleName();
            if(!imports.contains(rclass)){
                imports.add(rclass);
            }
            if(!imports.contains(UpdateValue.class.getName())){
                imports.add(UpdateValue.class.getName());
            }
            String rowVarName = this.realFuncName + "row" + arguments.size();
            StringBuilder rowDeclare = new StringBuilder();
            rowDeclare.append(rsclass + CODE_SEP + rowVarName + CODE_EQ + "new FunctionArgsRow(" + arguments.size() +")");
            if(!variables.contains(rowDeclare.toString())){
                variables.add(rowDeclare.toString());
            }
            sb.append(funcVar);
            sb.append(".eval(row.update(new UpdateValue[]{");
            Var arg = arguments.get(0);
            if (arg.getDataType() == Var.DataType.VAR) {
                Object value = null;
                try{
                    value = arg.getVarValue();
                } catch (ParseException e) {
                }
                if(value != null){
                    sb.append("new UpdateValue(");
                    sb.append(0);
                    sb.append(",\"");
                    sb.append(value.toString());
                    sb.append("\")");
                } else {
                    sb.append("new UpdateValue(");
                    sb.append(0);
                    sb.append(",");
                    sb.append(arg.getVarName());
                    sb.append(")");
                }
            } else  if (arg.getValueType() == Var.ValueType.EXPRESSION) {
                TreeNode baseStatement = arg.getExpr();
                if(baseStatement instanceof BaseStatement){
                    String code = ((BaseStatement) baseStatement).doCodegen(imports, variables, knownVars);
                    sb.append("new UpdateValue(");
                    sb.append(0);
                    sb.append(",");
                    sb.append(code);
                    sb.append(")");
                }
            } else {
                sb.append("new UpdateValue(");
                sb.append(0);
                sb.append(",\"");
                sb.append(arg.toString());
                sb.append("\")");
            }
            sb.append("}))");
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
