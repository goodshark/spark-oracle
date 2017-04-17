package org.apache.hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.util.StrUtils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhongdg1 on 2017/1/12.
 */
public class DateNameFunction extends BaseFunction {

    private DateUnit dateUnit;
    private TreeNode expr;

    public DateNameFunction(FuncName name) {
        super(name);
    }

    public void setDateUnit(DateUnit dateUnit) {
        this.dateUnit = dateUnit;
    }

    public void setExpr(TreeNode expr) {
        this.expr = expr;
    }

    @Override
    public int execute() throws Exception {
        List<Var> results = new ArrayList<>();
        expr.setExecSession(getExecSession());
        expr.execute();
        Var date = (Var) expr.getRs().getObject(0);
        results.add(new Var(dateUnit, Var.DataType.DEFAULT));
        results.add(date);
        System.out.println("Excuting function # " + this.getSql());
        doCall(results);
        return 0;
    }

    @Override
    public String getSql() {

        StringBuffer sb = new StringBuffer(getFunctionAliasName());
        Var v = new Var(expr.getSql(), Var.DataType.DATETIME);
        try {
            if(null == v.getVarValue()) {
                return sb.toString();
            }
            if(v.getVarValue().toString().charAt(0) == '@') {
                return sb.append("(").append(expr.getSql()).append(")").toString();
            }

            sb.append("(").append(StrUtils.addQuot(v.getDateStr())).append(")");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return sb.toString();
    }


    private String getFunctionAliasName() {
        String functionName = null;
        switch (this.dateUnit) {
            case WEEK:
                functionName = "weekofyear";
                break;
            case WEEKDAY:

            default:
                functionName = this.dateUnit.toString();
        }
        return functionName;
    }
}
