package hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 2/22 0022.
 */
public class IsNullFunction extends BaseFunction {
    private List<TreeNode> exprList;

    public IsNullFunction(FuncName name) {
        super(name);
    }

    public void setExprList(List<TreeNode> list) {
        exprList = list;
    }

    @Override
    public int execute() throws Exception {
        List<Var> argList = new ArrayList<Var>();
        for (TreeNode node: exprList) {
            node.setExecSession(getExecSession());
            node.execute();
            Var arg = (Var) node.getRs().getObject(0);
            argList.add(arg);
        }
        doCall(argList);
        return 0;
    }

    @Override
    public String getSql() {
        // transform isnull -> ifnull (spark ifnull equal sqlserver isnull)
        StringBuffer sb = new StringBuffer(FunctionAliasName.getFunctionAlias()
                .getFunctionAliasName(getName().getFullFuncName()));
        sb.append("(").append(exprList.get(0).getSql()).append(",")
        .append(exprList.get(1).getSql()).append(")");
        return sb.toString();
    }
}
