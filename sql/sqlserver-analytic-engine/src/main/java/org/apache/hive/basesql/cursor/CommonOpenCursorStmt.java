package org.apache.hive.basesql.cursor;

import org.apache.hive.tsql.arg.SystemVName;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.cursor.Cursor;
import org.apache.hive.tsql.exception.NotDeclaredException;

import java.sql.ResultSet;

/**
 * Created by dengrb1 on 7/18 0018.
 */
public abstract class CommonOpenCursorStmt extends BaseStatement {
    private String cursorName;
    private boolean isGlobal = false;
    protected CommonCursor cursor = null;

    public CommonOpenCursorStmt() {
    }

    public CommonOpenCursorStmt(String nodeName, String name, boolean global) {
        super(nodeName);
        cursorName = name;
        isGlobal = global;
    }

    @Override
    public int execute() throws Exception {
        cursor = (isGlobal ? findCursor(cursorName, true) : findCursor(cursorName));
        if (null == cursor) {
            throw new NotDeclaredException("Cursor not declared # " + cursorName);
        }
        if (Cursor.CursorStatus.OPENING == cursor.getStatus()) {
            throw new RuntimeException("Cursor already opened # " + cursorName);
        }
        cursor.setStatus(Cursor.CursorStatus.OPENING);
        preSqlExecute();
        TreeNode selectStm = cursor.getTreeNode();
        selectStm.setExecSession(getExecSession());
        selectStm.setAddResult(false);
        selectStm.execute();
        ResultSet rs = selectStm.getRs();
        cursor.setRs(rs);
        postExecute();
        return 0;
    }

    public abstract int preSqlExecute() throws Exception;

    public abstract int postExecute() throws Exception;

    @Override
    public BaseStatement createStatement() {
        return null;
    }
}
