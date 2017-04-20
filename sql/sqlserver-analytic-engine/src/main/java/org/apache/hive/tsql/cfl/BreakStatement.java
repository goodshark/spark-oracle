package org.apache.hive.tsql.cfl;

import org.apache.hive.basesql.cfl.NonSeqStatement;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.node.LogicNode;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class BreakStatement extends NonSeqStatement {

    public BreakStatement() {
    }

    public BreakStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public int execute() throws Exception {
        if (condition != null) {
            condition.execute();
            enable = condition.getBool();
        }
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }
}
