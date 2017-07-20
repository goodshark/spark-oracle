package org.apache.hive.basesql.cfl;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

/**
 * Created by dengrb1 on 5/25 0025.
 */
public abstract class CommonReturnStatement extends BaseStatement {
    private TreeNode expr = null;

    public CommonReturnStatement() {
        super();
    }

    public void setExpr(TreeNode e) {
        expr = e;
    }

    @Override
    public int execute() throws Exception {
        if (expr == null) {
            postExecute(null);
            return 0;
        }

        expr.setExecSession(getExecSession());
        expr.execute();
        Var result = (Var) expr.getRs().getObject(0);
        postExecute(result);
        return 0;
    }

    public abstract void postExecute(Var res) throws Exception;

    @Override
    public String doCodegen(){
        StringBuffer sb = new StringBuffer();
        sb.append("return ");
        BaseStatement bs = (BaseStatement)expr;
        sb.append(bs.doCodegen());
        sb.append(CODE_END);
        return sb.toString();
    }
}
