package org.apache.hive.basesql.cfl;

import org.apache.hive.tsql.cfl.WhileStatement;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.node.LogicNode;

import java.util.Set;

/**
 * Created by dengrb1 on 4/20 0020.
 */
public abstract class NonSeqStatement extends BaseStatement {
    protected String label = "";
    protected TreeNode condition = null;
    // is continue/break work
    protected boolean enable = true;

    public void setLabel(String name) {
        label = name;
    }

    public void setCondition(TreeNode node) {
        condition = node;
    }

    public boolean isEnable() {
        return enable;
    }

    public boolean isPair(TreeNode node) {
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
            if (label == null || label.isEmpty()) {
                return pNode;
            } else {
                WhileStatement loopStmt = (WhileStatement) pNode;
                if (loopStmt.existLabel(label))
                    return pNode;
                else
                    return findPairLoop(pNode);
                /*WhileStatement loopStmt = (WhileStatement) pNode;
                if (!loopStmt.isLabelSearched()) {
                    Set<String> labels = loopStmt.searchAllLabels();
                    loopStmt.setLabelSearched(labels);
                }
                if (loopStmt.existLabel(label))
                    return pNode;
                else
                    return findPairLoop(pNode);*/
            }
        }
    }
}
