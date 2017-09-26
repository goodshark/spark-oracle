package org.apache.hive.plsql.expression;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.dml.ExpressionStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 9/14 0014.
 */
public class MultiMemberExpr extends ExpressionStatement {
    private ExpressionStatement headExpr;
    // true: index, false: member
    private List<Boolean> markList = new ArrayList<>();
    private List<List<ExpressionStatement>> exprLists = new ArrayList<>();

    private boolean assignment = false;

    /**
     * a.b, a.b.c, a.b.c.d ... as normal var, is NOT parsed as MultiMemberExpr
     * a(1), a(x) ... as function call, is NOT parsed as MultiMemberExpr
     * a(1)(2), a.b(1), a.b.c(2), a.b(1).c, a.delete(), a.count ... parsed as MultiMemberExpr
     */
    public MultiMemberExpr() {
    }

    public void setHeadExpr(ExpressionStatement expr) {
        headExpr = expr;
    }

    public void mark(boolean m) {
        markList.add(m);
    }

    public void addExpr(List<ExpressionStatement> expressionStatements) {
        exprLists.add(expressionStatements);
    }

    public void setAssignment(boolean status) {
        assignment = status;
    }

    /**
     * for a.b(1).c
     * first search a as variable name, if a exists, b as a's member, c as b's member
     * if a as variable name do not exists in any block, then search a as label name, like <<a>> b(1).c
     */
    @Override
    public int execute() throws Exception {
        Var curVar = null;
        int index = 0;
        if (markList.size() != exprLists.size())
            throw new Exception("MultiMemberExpr index mark is not same as exprLists");
        if (exprLists.size() == 0)
            throw new Exception("MultiMemberExpr is made of empty expr");
        String firstVarName = headExpr.getExpressionBean().getVar().getVarName();
        curVar = findVar(firstVarName);
        if (curVar == null) {
            // first tag is the label-name
            if (exprLists.size() == 0 || markList.get(index))
                throw new Exception("MultiMemberExpr find var and find tag-var all failed");
            String labelVarName = firstVarName + "." + exprLists.get(index).get(0).getExpressionBean().getVar().getVarName();
            curVar = findVar(labelVarName);
            if (curVar == null)
                throw new Exception("MultiMemberExpr find var failed");
            index++;
        }

        for (; index < exprLists.size(); index++) {
            if (markList.get(index)) {
                // a(i)
                List<ExpressionStatement> exprs = exprLists.get(index);
                if (exprs.size() == 0 || exprs.size() > 1)
                    throw new Exception("empty call or more than 1 arg in multiMemberVar not support");
                exprs.get(0).setExecSession(getExecSession());
                exprs.get(0).execute();
                Var memberVar = (Var) exprs.get(0).getRs().getObject(0);
                if (curVar.getDataType() == Var.DataType.ASSOC_ARRAY)
                    curVar = getAssocArrayIndex(curVar, memberVar.getVarValue().toString());
                else {
                    int memberIndex = (int) memberVar.getVarValue();
                    curVar = getCollectionIndex(curVar, memberIndex);
                }
                if (curVar == null)
                    throw new Exception("MultiMemberExpr can not find index");
            } else {
                // a.b
                String memberName = exprLists.get(index).get(0).getExpressionBean().getVar().getVarName();
                Var methodVar = null;
                if (Var.isCollectionMethod(memberName)) {
                    // a.XXX, XXX is collectionMethod, get subsequent args
                    index++;
                    List<ExpressionStatement> argExprs = null;
                    if (index < exprLists.size())
                        argExprs = exprLists.get(index);
                    if (index < exprLists.size() || argExprs == null || argExprs.size() == 0) {
                        methodVar = Var.callCollectionMethod(curVar, memberName);
                    } else {
                        Object[] argObjs = new Object[argExprs.size()];
                        for (int i = 0; i < argExprs.size(); i++) {
                            argExprs.get(i).setExecSession(getExecSession());
                            argExprs.get(i).execute();
                            Var argVar = (Var) argExprs.get(i).getRs().getObject(0);
                            argObjs[i] = argVar.getVarValue();
                        }
                        methodVar = Var.callCollectionMethod(curVar, memberName, argObjs);
                    }
                }
                if (methodVar != null) {
                    setRs(new SparkResultSet().addRow(new Object[]{methodVar}));
                    return 0;
                }
                curVar = getCollectionMember(curVar, memberName);
                if (curVar == null)
                    throw new Exception("MultiMemberExpr can not find member: " + memberName);
            }
        }

        setRs(new SparkResultSet().addRow(new Object[] {curVar}));
        return 0;
    }

    /**
     * ASSOC_ARRAY
     */
    private Var getAssocArrayIndex(Var curVar, String index) throws Exception {
        if (assignment) {
            // need add a new index or overwrite old index
            return curVar.getAssocArrayValue(index, null);
        }
        return curVar.getAssocArrayValue(index);
    }

    /**
     * VARRAY, NESTED_TABLE
     */
    private Var getCollectionIndex(Var curVar, int index) throws Exception {
        if (curVar.getDataType() == Var.DataType.VARRAY) {
            return curVar.getVarrayInnerVar(index);
        } else if (curVar.getDataType() == Var.DataType.NESTED_TABLE) {
            return curVar.getNestedTableInnerVar(index);
        } else {
            throw new Exception("get collection index type error: " + curVar.getDataType());
        }
    }

    /**
     * COMPOSITE
     */
    private Var getCollectionMember(Var curVar, String tag) throws Exception {
        if (curVar.getDataType() != Var.DataType.COMPOSITE && curVar.getDataType() != Var.DataType.REF_COMPOSITE)
            throw new Exception("var " + curVar.getVarName() + " datatype is not match COMPOSITE: " + curVar.getDataType());
        return curVar.getInnerVar(tag);
    }
}
