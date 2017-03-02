package hive.tsql.cursor;

import org.apache.hive.tsql.common.BaseStatement;

/**
 * Created by zhongdg1 on 2016/12/28.
 */
public class DeallocateCursorStatement extends BaseStatement {
    private static final String STATEMENT_NAME = "_DEALLOCATE_CURSOR_";
    private String cursorName;
    private boolean isGlobal = false;

    public DeallocateCursorStatement(String cursorName, boolean isGlobal) {
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
        Cursor cursor = isGlobal ? findCursor(cursorName, true) : findCursor(cursorName);
        if (null == cursor) {
            System.out.println("Cursor not declared # " + cursorName);
            return 1;
        }
        //TODO 做close做的所有事情,清理结果集
        deleleCursor(cursorName, isGlobal);
        return 0;
    }
}
