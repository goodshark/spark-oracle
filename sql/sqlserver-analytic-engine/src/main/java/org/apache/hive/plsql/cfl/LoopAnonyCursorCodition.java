package org.apache.hive.plsql.cfl;

import org.apache.hive.plsql.cursor.OracleCursor;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.node.LogicNode;

/**
 * Created by dengrb1 on 8/28 0028.
 */
public class LoopAnonyCursorCodition extends LoopCursorConditionStmt {
    private TreeNode dmlNode;

    public void setDmlNode(TreeNode node) {
        dmlNode = node;
    }

    @Override
    protected void getCursor() throws Exception {
        // create anonymous cursor & add cursor into loop scope
        cursor = new OracleCursor(cursorName);
        cursor.setTreeNode(dmlNode);
        addCursor(cursor);
    }

}
