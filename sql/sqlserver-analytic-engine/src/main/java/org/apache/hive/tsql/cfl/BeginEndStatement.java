package org.apache.hive.tsql.cfl;

import org.apache.hive.plsql.function.ProcedureCall;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.spark.sql.catalyst.plfunc.PlFunctionRegistry;

import java.util.List;

/**
 * Created by dengrb1 on 12/8 0008.
 */
public class BeginEndStatement extends BaseStatement {
    private TreeNode block = null;

    public BeginEndStatement() {
    }

    public BeginEndStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public int execute() throws Exception {
        // this is a dumb node
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public String doCodegen(List<String> variables, List<String> childPlfuncs, PlFunctionRegistry.PlFunctionIdentify current, String returnType) throws Exception{
        StringBuffer sb = new StringBuffer();
        List<TreeNode> childs = getChildrenNodes();
        int i=0;
        for(TreeNode node : childs){
            i++;
            if(node instanceof GotoStatement && i < childs.size() && childs.get(i) instanceof WhileStatement){
                sb.append(((GotoStatement) node).getLabel());
                sb.append(":");
            } else {
                if(node instanceof ProcedureCall){
                    ProcedureCall bs = (ProcedureCall)node;
                    sb.append(bs.doCodegen(variables, childPlfuncs, current, returnType));
                    sb.append(CODE_END);
                    sb.append(CODE_LINE_END);
                } else if (node instanceof BaseStatement){
                    BaseStatement bs = (BaseStatement)node;
                    sb.append(bs.doCodegen(variables, childPlfuncs, current, returnType));
                    sb.append(CODE_LINE_END);
                }
            }
        }
        return sb.toString();
    }
}
