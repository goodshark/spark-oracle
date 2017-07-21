package org.apache.hive.tsql.cfl;

import org.apache.hive.basesql.cfl.NonSeqStatement;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.node.LogicNode;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class ContinueStatement extends NonSeqStatement {
    private String label = "";
    private LogicNode condition = null;
    // is continue work
    private boolean enable = true;

    public ContinueStatement() {
    }

    public ContinueStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    /*public void setLabel(String name) {
        label = name;
    }

    public void setCondition(LogicNode node) {
        condition = node;
    }

    public boolean isEnable() {
        return enable;
    }*/

    public int execute() throws Exception {
        if (condition != null) {
            condition.execute();
            enable = condition.getBool();
        }
        return 0;
    }

    /*public boolean isPair(TreeNode node) {
        TreeNode loopNode = findPairLoop(this);
        if (loopNode != null && loopNode.equals(node))
            return true;
        return false;
    }

    private TreeNode findPairLoop(TreeNode node) {
        if (node == null || node.getParentNode() == null)
            return null;
        TreeNode pNode = node.getParentNode();
        if (pNode.getNodeType() != TreeNode.Type.WHILE) {
            return findPairLoop(pNode);
        } else {
            if (label.isEmpty()) {
                return pNode;
            } else {
                WhileStatement loopStmt = (WhileStatement) pNode;
                if (!loopStmt.isLabelSearched()) {
                    loopStmt.searchAllLabels();
                }
                if (loopStmt.existLabel(label))
                        return pNode;
                    else
                        return findPairLoop(pNode);
            }
        }
    }*/

    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public String doCodegen(){
        StringBuffer sb = new StringBuffer();
        if(condition != null){
            sb.append("if(");
            sb.append(condition.doCodegen());
            sb.append("){");
            sb.append(CODE_LINE_END);
            if(label != null){
                sb.append("continue " + label);
            } else {
                sb.append("continue ");
            }
            sb.append(CODE_END);
            sb.append(CODE_LINE_END);
            sb.append("}");
            sb.append(CODE_LINE_END);
        } else {
            if(label != null){
                sb.append("continue " + label);
            } else {
                sb.append("continue ");
            }
            sb.append(CODE_END);
            sb.append(CODE_LINE_END);
        }
        return sb.toString();
    }
}
