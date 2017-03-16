package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.node.LogicNode;
import org.apache.hive.tsql.common.TreeNode;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class WhileStatement extends BaseStatement {

    private LogicNode condtionNode = null;

    public WhileStatement() {
    }

    public WhileStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void setCondtionNode(LogicNode node) {
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

    public int execute() throws Exception {
        return 0;
    }
    public BaseStatement createStatement() {
        return null;
    }
}
