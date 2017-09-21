package org.apache.hive.basesql.func;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.plsql.type.LocalTypeDeclare;
import org.apache.hive.plsql.type.NestedTableTypeDeclare;
import org.apache.hive.plsql.type.VarrayTypeDeclare;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dbservice.ProcService;
import org.apache.hive.tsql.dml.ExpressionStatement;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.exception.FunctionNotFound;
import org.apache.hive.tsql.exception.NotDeclaredException;
import org.apache.hive.tsql.func.FuncName;
import org.apache.hive.tsql.func.Procedure;
import org.apache.hive.tsql.util.StrUtils;
import org.apache.spark.sql.catalyst.plans.logical.Except;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 5/24 0024.
 */
public abstract class CallStatement extends ExpressionStatement {
    protected List<Var> arguments = new ArrayList<>();
    protected FuncName funcName;
    protected String realFuncName;
    protected CommonProcedureStatement func;
    protected int type = 1;

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
        funcName.setDatabase(getExecSession().getDatabase());
        if (funcName == null)
            throw new FunctionNotFound(realFuncName);
        if (funcName.isVariable()) {
            Var v = findVar(funcName.getRealFullFuncName());
            if (null == v) {
                throw new NotDeclaredException(funcName.getFuncName());
            }
            realFuncName = StrUtils.trimQuot(v.getVarValue().toString());
        }
        CommonProcedureStatement function = null;
        if (null == this.func) {
            function = super.findFunc(funcName.getRealFullFuncName(), arguments);
        } else {
            function = func;
        }
        if (null == function) {
            // 待测试
            ProcService procService = new ProcService(getExecSession().getSparkSession());
            int count = procService.getCountByName(funcName.getRealFullFuncName(), type);
            if (count == 1) {
                function = procService.getProcContent(funcName.getRealFullFuncName(), type);
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

    private void assignmentValue(Var rootVar, Var leftVar, Var rightVar) throws Exception {
        if (leftVar.getDataType() == Var.DataType.COMPOSITE) {
        } else if (leftVar.getDataType() == Var.DataType.VARRAY) {
            TreeNode expr = rightVar.getExpr();
            if (expr == null)
                throw new Exception("type constructor get null expr arg");
            expr.setExecSession(getExecSession());
            expr.execute();
            Var varrayVar = (Var) expr.getRs().getObject(0);
            Var.assign(leftVar, varrayVar);
        } else if (leftVar.getDataType() == Var.DataType.NESTED_TABLE) {
        } else {
            // base type
            Object val = getValueFromVar(rightVar);
            leftVar.setVarValue(val);
        }

        Var.DataType rootType = rootVar.getDataType();
        if (rootType == Var.DataType.VARRAY) {
            rootVar.addVarrayValue(leftVar);
        } else if (rootType == Var.DataType.NESTED_TABLE)
            rootVar.addNestedTableValue(leftVar);
        else if (rootType == Var.DataType.COMPOSITE || rootType == Var.DataType.REF_COMPOSITE)
            rootVar.addInnerVar(leftVar);
        else if (rootType == Var.DataType.ASSOC_ARRAY) {
        } else {
            // TODO
        }
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

    private boolean findTypeConstructor() throws Exception {
        // TODO only support base type
        LocalTypeDeclare typeDeclare = findType(funcName.getFuncName());
        if (typeDeclare != null) {
            Var resultVar = new Var();
            Var.DataType type = typeDeclare.getDeclareType();
            resultVar.setDataType(type);
            if (type == Var.DataType.VARRAY) {
                if (arguments.size() > ((VarrayTypeDeclare) typeDeclare).getSize())
                    throw new Exception("type constructor size is out of limit: " + arguments.size());
                // add value type into 0st
                Var typeVar = ((VarrayTypeDeclare)typeDeclare).getTypeVar();
                resultVar.addVarrayTypeVar(typeVar);
                for (Var arg: arguments) {
                    Var.DataType varrayValueType = ((VarrayTypeDeclare)typeDeclare).getVarrayValueType();
                    Var varrayVar = new Var();
                    Object val = getValueFromVar(arg);
                    varrayVar.setDataType(varrayValueType);
                    varrayVar.setVarValue(val);
                    resultVar.addVarrayValue(varrayVar);
                }
            } else if (type == Var.DataType.NESTED_TABLE) {
                Var typeVar = (typeDeclare).getTableTypeVar();
                resultVar.addNestedTableTypeVar(typeVar);
                for (Var arg: arguments) {
                    Var.DataType nestedTableValueType = ((NestedTableTypeDeclare)typeDeclare).getNestedTableValueType();
                    Var tableVar = new Var();
                    tableVar.setDataType(nestedTableValueType);
                    // TODO support collection type
                    assignmentValue(resultVar, tableVar, arg);
                    /*Object val = getValueFromVar(arg);
                    tableVar.setVarValue(val);
                    resultVar.addNestedTableValue(tableVar);*/
                }
            } else {
                throw new Exception("type constructor is error: " + funcName.getFuncName());
            }
            resultVar.setInitialized();
            setRs(new SparkResultSet().addRow(new Object[] {resultVar}));
            return true;
        }
        return false;
    }

    private boolean findVarSubscript() throws Exception {
        Var var = findVar(funcName.getFuncName());
        if (var != null) {
            if (var.getDataType() == Var.DataType.VARRAY) {
                if (arguments.size() != 1)
                    throw new Exception("var index is more than 1 in VARRAY type");
                int index = (int) getValueFromVar(arguments.get(0));
                setRs(new SparkResultSet().addRow(new Object[] {var.getVarrayInnerVar(index)}));
                return true;
            } else if (var.getDataType() == Var.DataType.NESTED_TABLE) {
                if (arguments.size() != 1)
                    throw new Exception("var index is more than 1 in NESTED_TABLE type");
                int index = (int) getValueFromVar(arguments.get(0));
                setRs(new SparkResultSet().addRow(new Object[] {var.getNestedTableInnerVar(index)}));
                return true;
            } else if (var.getDataType() == Var.DataType.ASSOC_ARRAY) {
                setRs(new SparkResultSet().addRow(new Object[] {var.getAssocArrayValue(getValueFromVar(arguments.get(0)).toString())}));
                return true;
            } else {
                throw new Exception("var " + funcName.getFuncName() + " subscript is error" );
            }
        }
        return false;
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
        // type-constructor first, TYPE init, a := TYPE(...)
        boolean findType = findTypeConstructor();
        if (findType)
            return 0;
        // varray and nested-table in g4, a(1) is function_call, a(1)(2)... is member_var
        boolean findVar = findVarSubscript();
        if (findVar)
            return 0;
        preExecute();
        call();
        postExecute();
        return 0;
    }

    public abstract void call() throws Exception;
}