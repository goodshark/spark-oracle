package hive.tsql.cfl;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class BreakStatement extends BaseStatement {
    public BreakStatement() {
    }

    public BreakStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public int execute() throws Exception {
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }
}
