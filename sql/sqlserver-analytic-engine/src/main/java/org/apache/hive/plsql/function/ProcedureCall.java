package org.apache.hive.plsql.function;

import org.apache.hive.basesql.func.CallStatement;
import org.apache.hive.basesql.func.CommonProcedureStatement;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dml.ExpressionStatement;
import org.apache.hive.tsql.exception.NotDeclaredException;
import org.apache.hive.tsql.execute.Executor;
import org.apache.hive.tsql.func.FuncName;
import org.apache.hive.tsql.func.Procedure;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dengrb1 on 5/24 0024.
 */

public class ProcedureCall extends CallStatement {
    private static final String STATEMENT_NAME = "_ORACLE_PROC_CALL_";

    public ProcedureCall(FuncName name) {
        super(STATEMENT_NAME);
        setFuncName(name);
        setRealFuncName(name.getFullFuncName());
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
}
