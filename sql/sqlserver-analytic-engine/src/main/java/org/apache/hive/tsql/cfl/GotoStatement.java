package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class GotoStatement extends BaseStatement {
    private boolean action = false;
    private String label = null;
    // for goto in TRY-CATCH block
    private TryCatchStatement tryCatchNode = null;
    private boolean belongCatchBlock = false;

    public GotoStatement() {
    }

    public GotoStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void setAction() {
        action = true;
    }

    public boolean getAction() {
        return action;
    }

    public void setLabel(String name) {
        label = name.toLowerCase();
    }

    public String getLabel() {
        return label;
    }

    public int execute() throws Exception {
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }

    public void setTryCatchNode(TryCatchStatement node) {
        tryCatchNode = node;
    }

    public TryCatchStatement getTryCatchNode() {
        return tryCatchNode;
    }

    public void setBelongCatchBlock() {
        belongCatchBlock = true;
    }

    public boolean isBelongCatchBlock() {
        return belongCatchBlock;
    }
}
