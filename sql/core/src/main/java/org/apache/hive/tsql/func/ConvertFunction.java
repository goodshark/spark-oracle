package org.apache.hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dml.ExpressionStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhongdg1 on 2017/1/12.
 */
public class ConvertFunction extends BaseFunction {
    private Var.DataType dataType;
    private TreeNode expr;
    private ExpressionStatement style;
    public ConvertFunction(FuncName name) {
        super(name);
    }

    public void setDataType(Var.DataType dataType) {
        this.dataType = dataType;
    }

    public void setExpr(TreeNode expr) {
        this.expr = expr;
    }

    @Override
    public int execute() throws Exception {
        expr.setExecSession(getExecSession());
        expr.execute();

        Var result = (Var)expr.getRs().getObject(0);
        result.setDataType(dataType);
        List<Var> results = new ArrayList<>();
        results.add(result);
        doCall(results);
        return 0;
    }

    @Override
    public String getSql() {
        return new StringBuffer().append("CAST")
                .append("(").append(expr.getSql()).append(" AS ").append(dataType).append(")").toString();
    }
}
