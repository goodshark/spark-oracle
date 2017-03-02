package org.apache.hive.tsql.cursor;

import org.apache.hive.tsql.arg.SystemVName;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.exception.NotDeclaredException;

import java.sql.ResultSet;

/**
 * Created by zhongdg1 on 2016/12/28.
 */
public class OpenCursorStatement extends BaseStatement {
    private static final String STATEMENT_NAME = "_OPEN_CURSOR_";
    private String cursorName;
    private boolean isGlobal = false;

    public OpenCursorStatement(String cursorName, boolean isGlobal) {
        super(STATEMENT_NAME);
        this.cursorName = cursorName.trim().toUpperCase();
        this.isGlobal = isGlobal;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public int execute() throws Exception {
        //如果open global则直接从global cursor查找，否则先找local
        Cursor cursor = isGlobal ? findCursor(cursorName, true) : findCursor(cursorName);
        if (null == cursor) {
            throw new NotDeclaredException("Cursor not declared # " + cursorName);
        }
        if (Cursor.CursorStatus.OPENING == cursor.getStatus()) {
            throw new RuntimeException("Cursor already opened # " + cursorName);
        }
        cursor.setStatus(Cursor.CursorStatus.OPENING);
//        addCursor(cursor);
        TreeNode selectStm = cursor.getTreeNode();
        selectStm.setExecSession(getExecSession());
        selectStm.setAddResult(false);
        selectStm.execute();
        ResultSet rs = selectStm.getRs();
        cursor.setRs(rs);
//        updateSys(SystemVName.FETCH_STATUS, 0);
        if (rs != null && rs.getRow() > 0) {
            updateSys(SystemVName.CURSOR_ROWS, rs.getRow());
        }


        return 0;
    }
}
