package hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhongdg1 on 2017/1/12.
 */
public class TSubstringFunction extends BaseFunction {

    private TreeNode leftExpr;
    private TreeNode rightExpr;
    private TreeNode midExpr;

    public void setMidExpr(TreeNode midExpr) {
        this.midExpr = midExpr;
    }

    public void setLeftExpr(TreeNode leftExpr) {
        this.leftExpr = leftExpr;
    }

    public void setRightExpr(TreeNode rightExpr) {
        this.rightExpr = rightExpr;
    }

    public TSubstringFunction(FuncName name) {
        super(name);
    }

    @Override
    public int execute() throws Exception {
        List<Var> results = getExprsValues();
//        System.out.println("Excuting function # " + this.getSql());
        doCall(results);
        return 0;
    }

    private List<Var> getExprsValues() throws Exception {
        List<Var> results = new ArrayList<>();
        leftExpr.setExecSession(getExecSession());
        leftExpr.execute();
        Var left = new Var(((Var) leftExpr.getRs().getObject(0)).getVarValue(), Var.DataType.STRING);


        midExpr.setExecSession(getExecSession());
        midExpr.execute();
        Var mid = new Var(((Var) midExpr.getRs().getObject(0)).getVarValue(), Var.DataType.INT);
        mid.setVarValue(mid.getInt() - 1);


        rightExpr.setExecSession(getExecSession());
        rightExpr.execute();
        Var right = new Var(((Var) rightExpr.getRs().getObject(0)).getVarValue(), Var.DataType.INT);
        results.add(left);
        results.add(mid);
        results.add(right);
        return results;
    }

    @Override
    public String getSql() {
        StringBuffer sb = new StringBuffer(FunctionAliasName.getFunctionAlias()
                .getFunctionAliasName(getName().getFullFuncName()));
        try {
            List<Var> vars = getExprsValues();
            sb.append("(").append(vars.get(0).getVarValue()).append(", ").append(vars.get(1).getVarValue()).append(", ").append(vars.get(2).getVarValue()).append(")");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb.toString();
    }


}
