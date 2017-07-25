package org.apache.hive.basesql.cursor;

import org.apache.hive.tsql.common.BaseStatement;

/**
 * Created by dengrb1 on 7/18 0018.
 */
public abstract class CommonCloseCursorStmt extends BaseStatement {
    private String cursorName;
    private boolean isGlobal = false;

    public CommonCloseCursorStmt() {
    }

    public CommonCloseCursorStmt(String nodeName, String name, boolean global) {
        super(nodeName);
        cursorName = name;
        isGlobal = global;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public int execute() throws Exception {
        CommonCursor cursor = (isGlobal ? findCursor(cursorName, true) : findCursor(cursorName));
        if (null == cursor) {
            System.out.println("Cursor not declared # " + cursorName);
            return 1;
        }
        if (CommonCursor.CursorStatus.OPENING != cursor.getStatus() && CommonCursor.CursorStatus.FETCHING != cursor.getStatus()) {
            System.out.println("Cursor not opening or fetching # " + cursorName);
            return 1;
        }
        cursor.setStatus(CommonCursor.CursorStatus.CLOSED);
        //TODO 清理结果集
        return 0;
    }
}
