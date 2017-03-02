package hive.tsql.cfl;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.node.LogicNode;

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
}

