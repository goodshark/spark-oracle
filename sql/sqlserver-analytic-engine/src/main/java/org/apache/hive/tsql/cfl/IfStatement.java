package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.node.LogicNode;
import org.apache.hive.tsql.common.TreeNode;

import java.util.List;

/**
 * Created by dengrb1 on 12/6 0006.
 */

public class IfStatement extends BaseStatement {

    private LogicNode condtionNode = null;

    public IfStatement() {
        super();
    }

    public IfStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
        setAtomic(true);
    }

    public void setCondtion(LogicNode node) {
        condtionNode = node;
    }

    public boolean isTrue() throws Exception {
        if (condtionNode != null) {
            condtionNode.setExecSession(getExecSession());
            condtionNode.execute();
            return condtionNode.getBool();
        } else {
            return false;
        }
    }

    public int execute() {
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public String doCodegen(){
        StringBuffer sb = new StringBuffer();
        List<TreeNode> childs = this.getChildrenNodes();
        if(childs.size() == 1){
            TreeNode child = childs.get(0);
            if(this.condtionNode instanceof BaseStatement && child instanceof BeginEndStatement){
                sb.append("if(");
                sb.append(condtionNode.doCodegen());
                sb.append("){");
                sb.append(CODE_LINE_END);
                sb.append(((BeginEndStatement) child).doCodegen());
                sb.append("}");
                sb.append(CODE_LINE_END);
            }
        }
        if(childs.size() == 2){
            TreeNode left = childs.get(0);
            TreeNode rift = childs.get(1);
            if(this.condtionNode instanceof BaseStatement){
                if(left instanceof BeginEndStatement && rift instanceof BeginEndStatement){
                    sb.append("if(");
                    sb.append(condtionNode.doCodegen());
                    sb.append("){");
                    sb.append(CODE_LINE_END);
                    sb.append(((BeginEndStatement) left).doCodegen());
                    sb.append("}");
                    sb.append(CODE_LINE_END);
                    sb.append("else{");
                    sb.append(CODE_LINE_END);
                    sb.append(((BeginEndStatement) rift).doCodegen());
                    sb.append("}");
                    sb.append(CODE_LINE_END);
                }
                if(left instanceof BeginEndStatement && rift instanceof IfStatement){
                    sb.append("if(");
                    sb.append(condtionNode.doCodegen());
                    sb.append("){");
                    sb.append(CODE_LINE_END);
                    sb.append(((BeginEndStatement) left).doCodegen());
                    sb.append("}");
                    sb.append(CODE_LINE_END);
                    sb.append("else ");
                    sb.append(((BeginEndStatement) rift).doCodegen());
                    sb.append(CODE_LINE_END);
                }
            }
        }
        return sb.toString();
    }
}

