package org.apache.hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 3/13 0013.
 */
public class LeftFunction extends BaseFunction {
    private List<TreeNode> exprList;

    public LeftFunction(FuncName name) {
        super(name);
    }

    public void setExprList(List<TreeNode> list) {
        exprList = list;
    }

    @Override
    public int execute() throws Exception {
        List<Var> argList = new ArrayList<Var>();
        for (TreeNode node: exprList) {
            node.setExecSession(getExecSession());
            node.execute();
            Var arg = (Var) node.getRs().getObject(0);
            argList.add(arg);
        }
        Var startIndex = new Var("startIndex", 0, Var.DataType.INT);
        argList.add(1, startIndex);
        doCall(argList);
        return 0;
    }

    @Override
    public String getSql() {
        StringBuffer sb = new StringBuffer(FunctionAliasName.getFunctionAlias()
                .getFunctionAliasName(getName().getFullFuncName()));
        sb.append("(");
        sb.append(exprList.get(0).getSql()).append(",");
        sb.append(" 0, ");
        sb.append(exprList.get(1).getSql());
        sb.append(")");
        return sb.toString();
    }
}
