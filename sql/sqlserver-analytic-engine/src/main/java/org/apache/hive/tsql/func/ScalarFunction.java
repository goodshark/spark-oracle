package org.apache.hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dml.ExpressionListStatement;

import java.util.List;

/**
 * Created by zhongdg1 on 2017/1/12.
 */
public class ScalarFunction extends BaseFunction {
    //变量函数参数
    private ExpressionListStatement exprs; //函数参数

    public ScalarFunction(FuncName name) {
        super(name);
    }

    public void setExprs(TreeNode exprs) {
        this.exprs = (ExpressionListStatement) exprs;
    }

    @Override
    public int execute() throws Exception {
        List<Var> results = null;
        if (null != exprs) {
            exprs.setExecSession(getExecSession());
            exprs.execute();
            Var resultVar = (Var) exprs.getRs().getObject(0);
//            results = (List<Var>) exprs.getRs().getObject(0);
            results = (List<Var>) resultVar.getVarValue();
        }
        System.out.println("Excuting function # " + this.getSql());
        doCall(results);
        return 0;
    }

    @Override
    public String getSql() {
        String functionName = FunctionAliasName.getFunctionAlias()
                .getFunctionAliasName(getName().getFullFuncName());
        StringBuffer sb = new StringBuffer(functionName);
        if (null == exprs || exprs.getExprs().size() == 0) {
            return sb.append("()").toString();
        }

        sb.append("(").append(optime(functionName, exprs.getSql())).append(")");
        return sb.toString();
    }

    private String optime(String functionName, String sql) {
        try {
            if (null != sql && Integer.parseInt(sql.trim()) == 0 && isDate(functionName.trim())) {
                return "'1900-1-1'";
            }
        } catch (Exception e) {
//            e.printStackTrace();
        }
        return sql;
    }

    private boolean isDate(String functionName) {
        String[] dateFunctions = new String[] {"day", "month", "year"};
        for(String name : dateFunctions) {
            if(functionName.equalsIgnoreCase(name)) {
                return true;
            }
        }

        return false;
    }
}
