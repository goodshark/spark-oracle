package org.apache.hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 4/18 0018.
 */
public class IifFunction extends BaseFunction {

    private List<TreeNode> exprList;

    public void setExprList(List<TreeNode> list) {
        exprList = list;
    }

    public IifFunction(FuncName name) {
        super(name);
    }

    @Override
    public int execute() throws Exception {
        List<Var> argList = new ArrayList<>();
        for (TreeNode node: exprList) {
            node.setExecSession(getExecSession());
            node.execute();
            Var arg = (Var) node.getRs().getObject(0);
            argList.add(arg);
        }
        doCall(argList);
        return 0;
    }

    @Override
    public String getSql() {
        StringBuffer sb = new StringBuffer();
        sb.append(" CASE WHEN ").append(exprList.get(0).getSql()).append(" THEN ");
        sb.append(exprList.get(1).getSql()).append(" ELSE ");
        sb.append(exprList.get(2).getSql());
        sb.append(" END ");
        return sb.toString();
    }
}
