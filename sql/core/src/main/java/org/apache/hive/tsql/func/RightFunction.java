package org.apache.hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.util.StrUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 3/13 0013.
 */
public class RightFunction extends BaseFunction {
    private List<TreeNode> exprList;
    private StringBuilder argSB = new StringBuilder();

    public RightFunction(FuncName name) {
        super(name);
    }

    public void setExprList(List<TreeNode> list) {
        exprList = list;
    }

    private List<Var> makeArgList() throws Exception {
        List<Var> argList = new ArrayList<Var>();
        for (TreeNode node: exprList) {
            node.setExecSession(getExecSession());
            node.execute();
            Var arg = (Var) node.getRs().getObject(0);
            argList.add(arg);
        }
        Var strVar = argList.get(0);
        String str = strVar.getVarValue().toString();
        Var lenVar = argList.get(1);
        if (lenVar == null || lenVar.getVarValue() == null || lenVar.getDataType() == Var.DataType.NULL)
            throw new Exception("right 2 arg is not number");
        int len = Integer.parseInt(lenVar.getVarValue().toString());
        String leftStr = str.substring(0, str.length() - len);
        String rightStr = str.substring(str.length() - len);
        String targetStr = rightStr + leftStr;
        argSB.append(StrUtils.addQuot(targetStr)).append(",");
        Var newStrVar = new Var("new str", targetStr, Var.DataType.STRING);
        argList.clear();
        argList.add(newStrVar);
        Var startIndex = new Var("startIndex", 0, Var.DataType.INT);
        argSB.append("0, ");
        argList.add(startIndex);
        Var lenArg = new Var("len", len, Var.DataType.INT);
        argSB.append(Integer.toString(len));
        argList.add(lenArg);
        return argList;
    }

    @Override
    public int execute() throws Exception {
        List<Var> argList = makeArgList();
        doCall(argList);
        return 0;
    }

    @Override
    public String getSql() {
        StringBuffer sb = new StringBuffer(FunctionAliasName.getFunctionAlias()
                .getFunctionAliasName(getName().getFullFuncName()));
        sb.append("(");
        try {
            makeArgList();
        } catch (Exception e) {
            // just log, do nothing
            System.out.print(e.getStackTrace());
        }
        sb.append(argSB.toString());
        sb.append(")");
        return sb.toString();
    }
}
