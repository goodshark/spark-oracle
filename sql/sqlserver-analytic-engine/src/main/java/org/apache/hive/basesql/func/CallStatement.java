package org.apache.hive.basesql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.dbservice.ProcService;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.exception.FunctionNotFound;
import org.apache.hive.tsql.exception.NotDeclaredException;
import org.apache.hive.tsql.func.FuncName;
import org.apache.hive.tsql.func.Procedure;
import org.apache.hive.tsql.util.StrUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 5/24 0024.
 */
public abstract class CallStatement extends BaseStatement {
    protected List<Var> arguments = new ArrayList<>();
    protected FuncName funcName;
    protected String realFuncName;
    protected CommonProcedureStatement func;

    public CallStatement() {
        super();
    }

    public CallStatement(String nodeName) {
        this();
        setNodeName(nodeName);
    }

    public void addArgument(Var var) {
        arguments.add(var);
    }

    private void checkArguments() {
        int leastInputSize = func.getLeastArguments();
        int inAndOutputSize = func.getInAndOutputs().size();
        int argumentSize = this.arguments.size();
        if (this.arguments.size() < leastInputSize || this.arguments.size() > inAndOutputSize) {
            throw new FunctionArgumentException(realFuncName, argumentSize, leastInputSize, inAndOutputSize);
        }
        extraCheckArg();
    }

    protected void extraCheckArg() {
    }

    protected FuncName getFuncName() {
        return funcName;
    }

    protected void setFuncName(FuncName name) {
        funcName = name;
    }

    protected String getRealFuncName() {
        return realFuncName;
    }

    protected void setRealFuncName(String fullName) {
        realFuncName = fullName;
    }

    protected void findFuncName() throws Exception {
        if (funcName == null)
            throw new FunctionNotFound(realFuncName);
        if (funcName.isVariable()) {
            Var v = findVar(funcName.getFuncName());
            if (null == v) {
                throw new NotDeclaredException(funcName.getFuncName());
            }
            realFuncName = StrUtils.trimQuot(v.getVarValue().toString());
        }
        CommonProcedureStatement function = null;
        if (null == this.func) {
            function = super.findFunc(realFuncName, arguments);
        } else {
            function = func;
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
        func = function;
    }

    protected void preExecute() throws Exception {
        findFuncName();
        checkArguments();
        saveScene();
    }

    protected void postExecute() throws Exception {
        recoveryScene();
    }

    @Override
    public int execute() throws Exception {
        preExecute();
        call();
        postExecute();
        return 0;
    }

    public abstract void call() throws Exception;
}