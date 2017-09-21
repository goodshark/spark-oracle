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
    // true: index, false: member
    private List<Boolean> markList = new ArrayList<>();
    private List<ExpressionStatement> exprs = new ArrayList<>();

    private boolean assignment = false;

    /**
     * a.b, a.b.c, a.b.c.d ... as normal var, is NOT parsed as MultiMemberExpr
     * a(1), a(x) ... as function call, is NOT parsed as MultiMemberExpr
     * a(1)(2), a.b(1), a.b.c(2), a.b(1).c ... parsed as MultiMemberExpr
     */
    public MultiMemberExpr() {
    }

    public void mark(boolean m) {
        markList.add(m);
    }

    public void addExpr(ExpressionStatement expr) {
        exprs.add(expr);
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
        if (markList.size() != exprs.size())
            throw new Exception("MultiMemberExpr index mark is not same as exprs");
        if (exprs.size() == 0)
            throw new Exception("MultiMemberExpr is made of empty expr");
        String firstVarName = exprs.get(index).getExpressionBean().getVar().getVarName();
        curVar = findVar(firstVarName);
        index++;
        if (curVar == null) {
            // first tag is the label-name
            if (exprs.size() < 2 || markList.get(index))
                throw new Exception("MultiMemberExpr find var and find tag-var all failed");
            String labelVarName = firstVarName + "." + exprs.get(index).getExpressionBean().getVar().getVarName();
            curVar = findVar(labelVarName);
            if (curVar == null)
                throw new Exception("MultiMemberExpr find var failed");
        }
        for (; index < exprs.size(); index++) {
            if (markList.get(index)) {
                // a(i)
                exprs.get(index).setExecSession(getExecSession());
                exprs.get(index).execute();
                Var memberVar = (Var) exprs.get(index).getRs().getObject(0);
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
                String memberName = exprs.get(index).getExpressionBean().getVar().getVarName();
                curVar = getCollectionMember(curVar, memberName);
                if (curVar == null)
                    throw new Exception("MultiMemberExpr can not find member: " + memberName);
            }
        }

//        setRs(new SparkResultSet().addRow(new Object[] {new Var("multiMemberVar", curVar, curVar.getDataType())}));
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
