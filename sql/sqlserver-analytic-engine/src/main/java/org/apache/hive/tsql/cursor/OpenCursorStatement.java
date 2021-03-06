package org.apache.hive.tsql.cursor;

import org.apache.hive.basesql.cursor.CommonCursor;
import org.apache.hive.basesql.cursor.CommonOpenCursorStmt;
import org.apache.hive.tsql.arg.SystemVName;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.exception.NotDeclaredException;

import java.sql.ResultSet;

/**
 * Created by zhongdg1 on 2016/12/28.
 */
public class OpenCursorStatement extends CommonOpenCursorStmt {
    private static final String STATEMENT_NAME = "_OPEN_CURSOR_";

    public OpenCursorStatement(String cursorName, boolean isGlobal) {
        super(STATEMENT_NAME, cursorName, isGlobal);
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public int preSqlExecute() throws Exception {
        return 0;
    }

    @Override
    public int postExecute() throws Exception {
        ResultSet rs = cursor.getRs();
        if (rs != null && rs.getRow() > 0) {
            updateSys(SystemVName.CURSOR_ROWS, rs.getRow());
        }
        return 0;
    }

    /*@Override
    public int execute() throws Exception {
        //如果open global则直接从global cursor查找，否则先找local
        CommonCursor cursor = (isGlobal ? findCursor(cursorName, true) : findCursor(cursorName));
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
    }*/
}
