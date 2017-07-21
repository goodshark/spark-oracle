package org.apache.hive.basesql.cursor;

import org.apache.hive.tsql.common.TreeNode;

import java.sql.ResultSet;

/**
 * Created by dengrb1 on 7/12 0012.
 */
public abstract class CommonCursor {
    public enum CursorStatus {
        DECLARED(1), OPENING(2), FETCHING(3), CLOSED(4), DEALLOCATED(5);

        CursorStatus(int code) {
        }
    }

    protected String name;
    protected TreeNode dmlStmt;

    private CursorStatus status = CursorStatus.DECLARED;
    private ResultSet rs;
    private boolean isGlobal = false;

    public CommonCursor() {
    }

    public CommonCursor(String n) {
        name = n;
    }

    public void setName(String name) {
        this.name = name.toUpperCase();
    }

    public String getName() {
        return name.toUpperCase();
    }

    public void setTreeNode(TreeNode treeNode) {
        dmlStmt = treeNode;
    }

    public TreeNode getTreeNode() {
        return dmlStmt;
    }

    public ResultSet getRs() {
        return rs;
    }

    public void setRs(ResultSet rs) {
        this.rs = rs;
    }

    public CursorStatus getStatus() {
        return status;
    }

    public void setStatus(CursorStatus status) {
        this.status = status;
    }

    public boolean isGlobal() {
        return isGlobal;
    }

    public void setGlobal(boolean global) {
        isGlobal = global;
    }
}
