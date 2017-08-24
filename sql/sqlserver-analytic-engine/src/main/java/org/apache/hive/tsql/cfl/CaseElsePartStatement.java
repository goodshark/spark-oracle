package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.spark.sql.catalyst.plfunc.PlFunctionRegistry;

import java.util.List;

/**
 * Created by chenfl2 on 2017/7/12.
 */
public class CaseElsePartStatement extends BaseStatement{

    public CaseElsePartStatement() {
        super();
    }

    public CaseElsePartStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public int execute() {
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public String doCodegen(List<String> variables, List<String> childPlfuncs, PlFunctionRegistry.PlFunctionIdentify current, String returnType) throws Exception{
        StringBuffer sb = new StringBuffer();
        sb.append("else {");
        sb.append(CODE_LINE_END);
        List<TreeNode> childs = getChildrenNodes();
        for(TreeNode child : childs){
            if(child instanceof BaseStatement){
                sb.append(((BaseStatement)child).doCodegen(variables, childPlfuncs, current, returnType));
                sb.append(CODE_LINE_END);
            }
        }
        sb.append("}");
        sb.append(CODE_LINE_END);
        return sb.toString();
    }

}
