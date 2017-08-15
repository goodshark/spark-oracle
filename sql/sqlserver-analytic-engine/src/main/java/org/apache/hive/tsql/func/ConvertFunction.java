package org.apache.hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.util.DateUtil;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhongdg1 on 2017/1/12.
 */
public class ConvertFunction extends BaseFunction {
    private Var.DataType dataType;
    private int length = -1;
    private TreeNode expr;
    private TreeNode style;

    public ConvertFunction(FuncName name) {
        super(name);
    }

    public void setDataType(Var.DataType dataType) {
        this.dataType = dataType;
    }

    public void setExpr(TreeNode expr) {
        this.expr = expr;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public void setStyle(TreeNode style) {
        this.style = style;
    }

    @Override
    public int execute() throws Exception {
        expr.setExecSession(getExecSession());
        expr.execute();

        Var result = ((Var) expr.getRs().getObject(0)).clone();
        String val = result.toString();
        if (val != null && style != null) {
            style.setExecSession(getExecSession());
            style.execute();
            String styleStr = ((Var) (style.getRs().getObject(0))).getVarValue().toString();
            val = DateUtil.format(DateUtil.parseLenient(val, "yyyy-MM-dd HH:mm:ss.SSS"), DateStyle.getInstance().getDateStyle(styleStr));
        }
        if (null != val && -1 != length && val.length() > length) {
            val = val.substring(0, length);
        }
        result.setDataType(dataType);
        result.setVarValue(val);
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
