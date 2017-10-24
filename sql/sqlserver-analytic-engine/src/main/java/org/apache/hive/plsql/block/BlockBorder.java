package org.apache.hive.plsql.block;

import org.apache.hive.tsql.common.TreeNode;

/**
 * Created by dengrb1 on 4/19 0019.
 */
public class BlockBorder extends TreeNode {
    private int blockHashCode = 0;
    private TreeNode node = null;

    public BlockBorder(TreeNode n) {
        node = n;
        blockHashCode = node.hashCode();
        setNodeType(TreeNode.Type.BORDER);
    }

    public boolean isSame(TreeNode n) {
        if (n.hashCode() != blockHashCode) {
            return false;
        } else {
            return node.equals(n);
        }
    }

    public boolean checkException(String name) {
        if (node.getNodeType() == TreeNode.Type.ANONY_BLOCK) {
            AnonymousBlock anonymousBlock = (AnonymousBlock) node;
            if (anonymousBlock.matchException(name))
                return true;
            else
                return false;
        } else {
            return false;
        }
    }

    public TreeNode getExceptionBlock(String name) {
        if (node.getNodeType() == TreeNode.Type.ANONY_BLOCK) {
            AnonymousBlock anonymousBlock = (AnonymousBlock) node;
            return anonymousBlock.getMatchHandler(name);
        } else
            return null;
    }

    public TreeNode getNode() {
        return node;
    }

    public int execute() throws Exception {
        return 0;
    }
}
