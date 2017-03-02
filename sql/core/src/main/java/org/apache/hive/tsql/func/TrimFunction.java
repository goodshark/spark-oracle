package hive.tsql.func;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 2/28 0028.
 */
public class TrimFunction extends BaseFunction {
    private List<TreeNode> exprList;

    public TrimFunction(FuncName name) {
        super(name);
    }

    public void setExprList(List<TreeNode> list) {

        exprList = list;
    }

    @Override
    public int execute() throws Exception {
        List<Var> argList = new ArrayList<Var>();
        for (TreeNode argNode: exprList) {
            argNode.setExecSession(getExecSession());
            argNode.execute();
            Var argVar = (Var) argNode.getRs().getObject(0);
            argList.add(argVar);
        }
        doCall(argList);
        return 0;
    }

    @Override
    public String getSql() {
        // spark only support trim(xxx)
        StringBuffer sb = new StringBuffer(FunctionAliasName.getFunctionAlias()
                .getFunctionAliasName(getName().getFullFuncName()));
        if (exprList.size() == 1)
            sb.append("(").append(exprList.get(0).getSql()).append(")");
        else
            sb.append("(").append(exprList.get(1).getSql()).append(")");
        return sb.toString();
    }
}
