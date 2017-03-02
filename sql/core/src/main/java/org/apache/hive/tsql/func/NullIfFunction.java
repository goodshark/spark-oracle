package hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhongdg1 on 2017/1/12.
 */
public class NullIfFunction extends BaseFunction {

    private TreeNode leftExpr;
    private TreeNode rightExpr;


    public void setLeftExpr(TreeNode leftExpr) {
        this.leftExpr = leftExpr;
    }

    public void setRightExpr(TreeNode rightExpr) {
        this.rightExpr = rightExpr;
    }

    public NullIfFunction(FuncName name) {
        super(name);
    }

    @Override
    public int execute() throws Exception {
        List<Var> results = new ArrayList<>();
        leftExpr.setExecSession(getExecSession());
        leftExpr.execute();
        Var left = (Var) leftExpr.getRs().getObject(0);
        rightExpr.setExecSession(getExecSession());
        rightExpr.execute();
        Var right = (Var) rightExpr.getRs().getObject(0);
        results.add(left);
        results.add(right);
        System.out.println("Excuting function # " + this.getSql());
        doCall(results);
        return 0;
    }

    @Override
    public String getSql() {

        StringBuffer sb = new StringBuffer(FunctionAliasName.getFunctionAlias()
                .getFunctionAliasName(getName().getFullFuncName()));
        sb.append("(").append(leftExpr.getSql()).append(", ").append(rightExpr.getSql()).append(")");
        return sb.toString();
    }
}
