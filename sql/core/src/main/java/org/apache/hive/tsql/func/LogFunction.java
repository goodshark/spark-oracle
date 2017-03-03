package org.apache.hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 2/28 0028.
 */
public class LogFunction extends BaseFunction {
    private TreeNode expr;

    public LogFunction(FuncName name) {
        super(name);
    }


    public void setExpr(TreeNode expr) {
        this.expr = expr;
    }

    @Override
    public int execute() throws Exception {
        List<Var> argList = new ArrayList<Var>();
        expr.setExecSession(getExecSession());
        expr.execute();
        Var argVar = (Var) expr.getRs().getObject(0);
        argList.add(argVar);
        doCall(argList);
        return 0;
    }

    @Override
    public String getSql() {
        // spark only support trim(xxx)
        StringBuffer sb = new StringBuffer(FunctionAliasName.getFunctionAlias()
                .getFunctionAliasName(getName().getFullFuncName()));
        sb.append("(").append(expr.getSql()).append(")");
        return sb.toString();
    }
}
