package org.apache.hive.tsql.cursor;

import org.apache.hive.basesql.cursor.CommonCloseCursorStmt;
import org.apache.hive.tsql.common.BaseStatement;

/**
 * Created by zhongdg1 on 2016/12/28.
 */
public class CloseCursorStatement extends CommonCloseCursorStmt {
    private static final String STATEMENT_NAME = "_CLOSE_CURSOR_";

    public CloseCursorStatement(String cursorName, boolean isGlobal) {
        super(STATEMENT_NAME, cursorName, isGlobal);
    }

    /*@Override
    public int execute() throws Exception {
        Cursor cursor = (Cursor) (isGlobal ? findCursor(cursorName, true) : findCursor(cursorName));
        if (null == cursor) {
            System.out.println("Cursor not declared # " + cursorName);
            return 1;
        }
        if (Cursor.CursorStatus.OPENING != cursor.getStatus() && Cursor.CursorStatus.FETCHING != cursor.getStatus()) {
            System.out.println("Cursor not opening or fetching # " + cursorName);
            return 1;
        }
        cursor.setStatus(Cursor.CursorStatus.CLOSED);
        //TODO 清理结果集
        return 0;
    }*/
}
