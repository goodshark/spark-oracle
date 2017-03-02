package hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhongdg1 on 2017/2/8.
 */
public class DateAddFunction extends BaseFunction {
    private String datePart;
    private TreeNode number;
    private TreeNode date;

    public DateAddFunction(FuncName name) {
        super(name);
    }

    public void setDatePart(String datePart) {
        this.datePart = datePart;
    }

    public void setNumber(TreeNode number) {
        this.number = number;
    }

    public void setDate(TreeNode date) {
        this.date = date;
    }

    @Override
    public int execute() throws Exception {
        List<Var> results = new ArrayList<>();
        number.setExecSession(getExecSession());
        number.execute();
        Var num = (Var) number.getRs().getObject(0);
        date.setExecSession(getExecSession());
        date.execute();
        Var oldDate = (Var) date.getRs().getObject(0);

        results.add(new Var(datePart, Var.DataType.STRING));
        results.add(num);
        results.add(oldDate);
        System.out.println("Excuting function # " + this.getSql());
        doCall(results);
        return 0;
    }

    @Override
    public String getSql() {

        StringBuffer sb = new StringBuffer(FunctionAliasName.getFunctionAlias()
                .getFunctionAliasName(getName().getFullFuncName()));
        sb.append("(").append(date.getSql()).append(", ").append(number.getSql()).append(")");
        return sb.toString();
    }
}
