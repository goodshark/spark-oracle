package org.apache.hive.tsql.another;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.dbservice.ProcService;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.exception.FunctionArgumentMismatchException;
import org.apache.hive.tsql.exception.FunctionNotFound;
import org.apache.hive.tsql.exception.NotDeclaredException;
import org.apache.hive.tsql.execute.Executor;
import org.apache.hive.tsql.func.FuncName;
import org.apache.hive.tsql.func.Procedure;
import org.apache.hive.tsql.util.StrUtils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zhongdg1 on 2016/12/13.
 */
public class ExecuteStatement extends BaseStatement {
    private static final String STATEMENT_NAME = "_EXEC_";

    private FuncName funcName; //Or proc name
    private List<Var> arguments = new ArrayList<>();
    private Procedure func;
    private String returnVarName = null;
    private String realFuncName;


    public ExecuteStatement(FuncName funcName) {
        super(STATEMENT_NAME);
        this.funcName = funcName;
        realFuncName = funcName.getRealFullFuncName();
    }

    public void setReturnVarName(String returnVarName) {
        this.returnVarName = returnVarName;
    }

    public void addArgument(Var var) {
        this.arguments.add(var);
    }

    public void setFunc(Procedure func) {
        this.func = func;
    }

    @Override
    public int execute() throws Exception {

        if (funcName.isVariable()) {
            Var v = findVar(funcName.getFuncName());
            if (null == v) {
                throw new NotDeclaredException(funcName.getFuncName());
            }
            realFuncName = StrUtils.trimQuot(v.getVarValue().toString());
        }
        Procedure function = null;
        if (null == this.func) {
            function = super.findFunc(realFuncName);
        }
        if (null == function) {
            // 待测试
            ProcService procService = new ProcService(getExecSession().getSparkSession());
            int count = procService.getCountByName(realFuncName);
            if (count == 1) {
                function = procService.getProcContent(realFuncName);
                //1表示从数据库读取
                function.setProcSource(1);
                addFunc(function);
            }
        }
        if (null == function) {
            throw new FunctionNotFound(realFuncName);
        }
        setFunc(function);
        checkArguments();

        //For input-----------------------------------------------
        int argumentSize = this.arguments.size();

        saveScene();

        Map<String, String> assigned = new HashMap<>();//保存已经被赋值的function变量

//        if (true) {
        for (int i = 0; i < argumentSize; i++) {
            Var argument = this.arguments.get(i);
            String argName = argument.getVarName();
            Var funcVar = null;
            funcVar = argName == null ? function.getInAndOutputs().get(i).clone() : findInputVar(argName);
            if (null == funcVar) {
                throw new NotDeclaredException(argName);
            }
            assignToFunc(argument, funcVar);
            assigned.put(funcVar.getVarName(), funcVar.getVarName());
            addVar(funcVar);//将input var将入变量容器
        }

        addUnassignArguments(function, assigned);

        //For return----------------------------------
        Var returnVar = null;
        if (null != this.returnVarName) {
            returnVar = findVar(returnVarName);
            if (null == returnVar) {
                throw new NotDeclaredException(returnVarName);
            }
        }

        int rsCount = getExecSession().getResultSets().size();
        new Executor(getExecSession(), function.getSqlClauses()).run();

        if (null != returnVar) {
            Var v = getReturnVal();
            if (v.isExecuted()) {
                updateVarValue(returnVar.getVarName(), v.getVarValue()); //将return实参值
            }
        }

        List<Var> newOutputs = new ArrayList<>();
        for (int i = 0; i < argumentSize; i++) {
            Var v = this.arguments.get(i);
            if (v.getVarType() == Var.VarType.OUTPUT) {
                Var newOutput = findVar(func.getInAndOutputs().get(i).getVarName());
                newOutput.setVarName(v.getVarValue().toString());
                if (null != newOutput) {
                    newOutputs.add(newOutput);
                }
            }
        }

        recoveryScene();
        for (Var v : newOutputs) {
            addVar(v);
        }
        if (null != returnVar) {
            addVar(returnVar);
        }
        int newRsCount = getExecSession().getResultSets().size();
        if (!isCollectRs() && newRsCount > rsCount) {
            this.setRs(getExecSession().getResultSets().remove(newRsCount - 1));
        }
        return 0;
    }

    private void addUnassignArguments(Procedure function, Map<String, String> assigned) {
        for (Var vv : function.getInAndOutputs()) {
            if (null != assigned.get(vv.getVarName().toUpperCase())) {
                continue;
            }
            addVar(vv.clone());
        }
    }

    private void assignToFunc(Var argument, Var funcVar) throws ParseException {
        Object val = argument.getVarValue();
        if (argument.getValueType() == Var.ValueType.EXPRESSION) {//如果是变量
            Var realVal = findVar(val.toString());
            if (null == realVal) {
                throw new NotDeclaredException(val.toString());
            }
            funcVar.setVarValue(realVal.getVarValue());
        } else {
            if (argument.getValueType() != Var.ValueType.DEFAULT) {
                funcVar.setVarValue(argument.getVarValue());
            }
        }
    }

    private void checkArguments() {
        //check must arguments
        int leastInputSize = func.getLeastArguments();
        int inAndOutputSize = func.getInAndOutputs().size();
        int argumentSize = this.arguments.size();
        if (this.arguments.size() < leastInputSize || this.arguments.size() > inAndOutputSize) {
            throw new FunctionArgumentException(realFuncName, argumentSize, leastInputSize, inAndOutputSize);
        }

        //check argument type
        for (int i = 0; i < argumentSize; i++) {
            //实参传的是output/而定义的形参不是output
            Var fVar = func.getInAndOutputs().get(i);
            if (arguments.get(i).getVarType() == Var.VarType.OUTPUT && fVar.getVarType() != Var.VarType.OUTPUT) {
                throw new FunctionArgumentMismatchException(realFuncName, fVar.getVarName(), "OUTPUT", "INPUT");
            }
        }

    }

    private Var findInputVar(String name) {
        for (Var var : this.func.getInAndOutputs()) {
            if (var.getVarName().equals(name)) {
                return var.clone();
            }
        }
        return null;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }

}
