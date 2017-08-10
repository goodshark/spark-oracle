package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.node.LogicNode;
import sun.reflect.generics.tree.Tree;

import java.util.List;

/**
 * Created by chenfl2 on 2017/7/12.
 */
public class CaseStatement extends BaseStatement {

    private String switchVar = null;
    private boolean isSimple = true;

    public CaseStatement(boolean isSimple) {
        super();
        this.isSimple = isSimple;
    }

    public CaseStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void setSwitchVar(String node) {
        switchVar = node;
    }

    public String getSwitchVar() {return switchVar;}

    public boolean isSimple(){return isSimple;}

    public int execute() {
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public String doCodegen(List<String> variables, List<String> childPlfuncs) throws Exception{
        StringBuffer sb = new StringBuffer();
        List<TreeNode> childs = getChildrenNodes();
        for(TreeNode child : childs){
            if(child instanceof BaseStatement){
                sb.append(((BaseStatement)child).doCodegen(variables, childPlfuncs));
            }
        }
        return sb.toString();
    }
}
