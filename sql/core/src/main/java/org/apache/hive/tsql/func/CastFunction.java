package org.apache.hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dml.ExpressionStatement;
import org.apache.hive.tsql.udf.UdfFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhongdg1 on 2017/1/12.
 */
public class CastFunction extends BaseFunction {

    private TreeNode expr;
    private Var.DataType dataType;

    public CastFunction(FuncName name) {
        super(name);
    }

    public void setExpr(TreeNode expr) {
        this.expr = expr;
    }

    public void setDataType(Var.DataType dataType) {
        this.dataType = dataType;
    }

    public TreeNode getExpr() {
        return expr;
    }

    @Override
    public int execute() throws Exception {
        expr.setExecSession(getExecSession());
        expr.execute();

        Var result = ((Var)expr.getRs().getObject(0)).clone();
        result.setDataType(dataType);
        List<Var> results = new ArrayList<>();
        results.add(result);
        doCall(results);
        return 0;
    }

    @Override
    public String getSql() {
        return new StringBuffer().append(getName().getFullFuncName())
                .append("(").append(expr.getSql()).append(" AS ").append(dataType).append(")").toString();
    }
}
