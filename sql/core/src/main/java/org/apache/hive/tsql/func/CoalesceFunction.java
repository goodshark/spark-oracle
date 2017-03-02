package org.apache.hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dml.ExpressionListStatement;

import java.util.List;

/**
 * Created by zhongdg1 on 2017/1/12.
 */
public class CoalesceFunction extends BaseFunction {
    //变量函数参数
    private TreeNode exprs; //函数参数

    public CoalesceFunction(FuncName name) {
        super(name);
    }

    public void setExprs(TreeNode exprs) {
        this.exprs = exprs;
    }

    @Override
    public int execute() throws Exception {
        List<Var> results = null;
        if (null != exprs) {
            exprs.setExecSession(getExecSession());
            exprs.execute();
            results = (List<Var>) exprs.getRs().getObject(0);
        }
        System.out.println("Excuting function # " + this.getSql());
        doCall(results);
        return 0;
    }

    @Override
    public String getSql() {

        StringBuffer sb = new StringBuffer(FunctionAliasName.getFunctionAlias()
                .getFunctionAliasName(getName().getFullFuncName()));
        if (null == exprs) {
            return sb.append("()").toString();
        }
        sb.append("(").append(exprs.getSql()).append(")");
        return sb.toString();
    }
}
