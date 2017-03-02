package hive.tsql.cfl;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class ContinueStatement extends BaseStatement {
    public ContinueStatement() {
    }

    public ContinueStatement(TreeNode.Type t) {
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
