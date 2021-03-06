package org.apache.hive.tsql.another;

import org.apache.hive.plsql.expression.MultiMemberExpr;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.AssignmentOp;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.cursor.Cursor;
import org.apache.hive.tsql.dml.ExpressionStatement;
import org.apache.hive.tsql.exception.NotDeclaredException;
import org.apache.spark.sql.catalyst.plfunc.PlFunctionRegistry;
import org.glassfish.jersey.message.internal.StringBuilderUtils;

import java.util.List;

/**
 * Created by zhongdg1 on 2016/12/2.
 */
public class SetStatement extends BaseStatement {

    private static final String STATEMENT_NAME = "_SET_";
    private Var.DataType dt = Var.DataType.COMMON;
    private AssignmentOp aop = AssignmentOp.EQ;

    public SetStatement() {
        super(STATEMENT_NAME);
    }

    private Var var;

    public void setAop(AssignmentOp aop) {
        this.aop = aop;
    }

    public void setVar(Var var) {
        this.var = var;
    }

    @Override
    public int execute() throws Exception {
        Cursor cursor = null;
        if (var.getValueType() == Var.ValueType.EXPRESSION) {
            cursor = (Cursor) findCursor(var.getVarName());
        }
        if (cursor != null) {
            var.setValueType(Var.ValueType.CURSOR);
        }


        switch (var.getValueType()) {
            case EXPRESSION:
                Var v = findVar(var.getVarName());
                if (v == null) {
                    // for a(1)(2) | a.b(1), varName is ""
                    MultiMemberExpr leftExpr = var.getLeftExpr();
                    if (leftExpr == null)
                        throw new NotDeclaredException(var.getVarName());
                    leftExpr.setAssignment(true);
                    leftExpr.setExecSession(getExecSession());
                    leftExpr.execute();
                    leftExpr.setAssignment(false);
                    v = leftExpr.getExpressionValue();
                }
                if (v.isReadonly())
                    throw new Exception("expression " + var.getVarName() + " cannot be used as an assignment target");
                if (v == null) {
                    throw new NotDeclaredException(var.getVarName());
                }
                assign(v);
                break;
            case CURSOR:
                if (null == cursor && var.getDataType() == Var.DataType.CURSOR) {
                    cursor = (Cursor) var.getVarValue();
                } else {
                    String realCursorName = var.getExpr().getSql().trim().toUpperCase();
                    Cursor realCursor = (Cursor) findCursor(realCursorName);
                    if (null == realCursor) {
                        throw new NotDeclaredException(realCursorName);
                    }
                    cursor.setTreeNode(realCursor.getTreeNode());
                }

//                if (null == findCursor(cursor.getName())) {
//                    System.out.println("Cursor not declared # " + cursor.getName());
//                    return -1;
//                }
                addCursor(cursor);
                break;
            case SPECIAL:
                break;
            default:
                break;
        }

        return 0;
    }

    private void assign(Var v) throws Exception {
        TreeNode stm = var.getExpr();
        stm.setExecSession(getExecSession());
        stm.execute();
        Var exprVal = stm.getExpressionValue();
        Object value = null;
        switch (aop) {
            /*case EQ:
                v.setVarValue(exprVal.getVarValue());
                break;
            case ADD_EQ:
                v.setVarValue(v.operatorAdd(exprVal).getVarValue());
                break;
            case SUB_EQ:
                v.setVarValue(v.operatorSub(exprVal).getVarValue());
                break;
            case MUL_EQ:
                v.setVarValue(v.operatorMultiply(exprVal).getVarValue());
                break;
            case DIV_EQ:
                v.setVarValue(v.operatorDiv(exprVal).getVarValue());
                break;
            case MOD_EQ:
                v.setVarValue(v.operatorMod(exprVal).getVarValue());
                break;
            case AND_EQ:
                v.setVarValue(v.operatorAnd(exprVal).getVarValue());
                break;
            case NOT_EQ:
                v.setVarValue(v.operatorXor(exprVal).getVarValue());
                break;
            case OR_EQ:
                v.setVarValue(v.operatorOr(exprVal).getVarValue());
                break;*/
            case EQ:
                value = exprVal.getVarValue();
                break;
            case ADD_EQ:
                value = v.operatorAdd(exprVal).getVarValue();
                break;
            case SUB_EQ:
                value = v.operatorSub(exprVal).getVarValue();
                break;
            case MUL_EQ:
                value = v.operatorMultiply(exprVal).getVarValue();
                break;
            case DIV_EQ:
                value = v.operatorDiv(exprVal).getVarValue();
                break;
            case MOD_EQ:
                value = v.operatorMod(exprVal).getVarValue();
                break;
            case AND_EQ:
                value = v.operatorAnd(exprVal).getVarValue();
                break;
            case NOT_EQ:
                value = v.operatorXor(exprVal).getVarValue();
                break;
            case OR_EQ:
                value = v.operatorOr(exprVal).getVarValue();
                break;
        }
        exprVal.setVarValue(value);
        Var.assign(v, exprVal);
        v.setExecuted(true);
    }


    @Override
    public BaseStatement createStatement() {
        return this;
    }

    @Override
    public String doCodegen(List<String> variables, List<String> childPlfuncs, PlFunctionRegistry.PlFunctionIdentify current, String returnType) throws Exception{
        StringBuffer sb = new StringBuffer();
        String varName = var.getVarName();
        String op = aop.val;
        String result = ((BaseStatement)var.getExpr()).doCodegen(variables, childPlfuncs, current, returnType);
        sb.append(varName);
        sb.append(op);
        sb.append(result);
        sb.append(BaseStatement.CODE_END);
        return sb.toString();
    }

}
