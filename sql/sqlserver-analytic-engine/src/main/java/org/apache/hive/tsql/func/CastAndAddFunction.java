package org.apache.hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhongdg1 on 2017/1/12.
 */
public class CastAndAddFunction extends BaseFunction {

    private TreeNode expr;
    private int incr;
    private TimeUnit timeUnit = TimeUnit.DAYS;

    public CastAndAddFunction(FuncName name) {
        super(name);
    }

    public void setExpr(TreeNode expr) {
        this.expr = expr;
    }

    public void setIncr(int incr) {
        this.incr = incr;
    }

    public void setTimeUnit(TimeUnit timeUnit) {
        this.timeUnit = timeUnit;
    }

    @Override
    public int execute() throws Exception {
        expr.setExecSession(getExecSession());
        expr.execute();

        Var result = ((Var) expr.getRs().getObject(0)).clone();
        List<Var> results = new ArrayList<>();
        results.add(result);
        doCall(results);
        return 0;
    }

    @Override
    public String getSql() {
        StringBuffer sb = new StringBuffer(FunctionAliasName.getFunctionAlias()
                .getFunctionAliasName(getName().getFullFuncName()));
        sb.append("(").append(getExecSql(expr)).append(", ").append(incr).append(")");
        return sb.toString();
    }

    private String getExecSql(TreeNode expr) {
        return expr instanceof CastFunction ? ((CastFunction) expr).getExpr().getSql() : expr.getSql();
    }
}
