package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

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
    public String doCodegen(){
        StringBuffer sb = new StringBuffer();
        List<TreeNode> childs = getChildrenNodes();
        for(TreeNode node : childs){
            if(node instanceof BaseStatement){
                BaseStatement bs = (BaseStatement)node;
                sb.append(bs.doCodegen());
                sb.append(CODE_LINE_END);
            }
        }
        return sb.toString();
    }
}
