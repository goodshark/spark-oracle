package org.apache.hive.tsql.func;

import org.apache.commons.lang3.StringUtils;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.util.StrUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhongdg1 on 2017/1/12.
 */
public class LenFunction extends BaseFunction {

    private TreeNode expr;

    public void setExpr(TreeNode expr) {
        this.expr = expr;
    }

    public LenFunction(FuncName name) {
        super(name);
    }

    @Override
    public int execute() throws Exception {
        List<Var> results = new ArrayList<>();
        expr.setExecSession(getExecSession());
        expr.setAddResult(false);
        expr.execute();
        Var val = new Var(((Var) expr.getRs().getObject(0)).getVarValue(), Var.DataType.STRING);

        results.add(val);
        doCall(results);
        return 0;
    }

    @Override
    public String getSql() {
        StringBuffer sb = new StringBuffer(FunctionAliasName.getFunctionAlias()
                .getFunctionAliasName(getName().getFullFuncName()));
        try {
//            expr.setExecSession(getExecSession());
//            expr.setAddResult(false);
//            expr.execute();
//            Var val = new Var(((Var) expr.getRs().getObject(0)).getVarValue(), Var.DataType.STRING);
//            sb.append("(").append(StrUtils.addQuot(StrUtils.trimRight(val.getVarValue().toString()))).append(")");
            sb.append("(").append(expr.getSql()).append(")");

        } catch (Exception e) {
            e.printStackTrace();
        }
        return sb.toString();
    }


}
