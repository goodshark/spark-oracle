package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class TryCatchStatement extends BaseStatement {
    private boolean executed = false;
    // only use for goto up-search parent node
    private boolean catchBlock = false;

    public TryCatchStatement() {
    }

    public TryCatchStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void setExecuted() {
        executed = true;
    }

    public boolean isExecuted() {
        return executed;
    }

    public int execute() throws Exception {
        return 0;
    }

    public void setCatchBlock() {
        catchBlock = true;
    }

    public boolean isCatchBlock() {
        return catchBlock;
    }

    public void clearBlockStatus() {
        catchBlock = false;
    }

    public BaseStatement createStatement() {
        return null;
    }
}
