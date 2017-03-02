package hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.udf.UdfFactory;

import java.util.List;

/**
 * Created by zhongdg1 on 2017/1/12.
 */
public abstract class BaseFunction extends TreeNode {
    private FuncName name;
    private int minArgsCount = 0;
    private int maxArgsCount = Integer.MAX_VALUE;

    public BaseFunction(FuncName name) {
        this.name = name;
    }

    public BaseFunction(FuncName name, int minArgsCount, int maxArgsCount) {
        this.name = name;
        this.minArgsCount = minArgsCount;
        this.maxArgsCount = maxArgsCount;
    }

    public FuncName getName() {
        return name;
    }

    public void setName(FuncName name) {
        this.name = name;
    }

    public void setMinArgsCount(int minArgsCount) {
        this.minArgsCount = minArgsCount;
    }

    public void setMaxArgsCount(int maxArgsCount) {
        this.maxArgsCount = maxArgsCount;
    }

    public int getMinArgsCount() {
        return minArgsCount;
    }

    public int getMaxArgsCount() {
        return maxArgsCount;
    }

    protected void doCall(List<Var> results) throws Exception{
        Var result = UdfFactory.getUdfFactory().createCalculator(getName().getFullFuncName().toUpperCase())
                .setExecSession(getExecSession()).setArguments(results).doComputing();
        this.setRs(new SparkResultSet().addRow(new Object[] {result}));
    }
}
