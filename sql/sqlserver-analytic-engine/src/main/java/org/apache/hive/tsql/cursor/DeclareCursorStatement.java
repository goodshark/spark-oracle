package org.apache.hive.tsql.cursor;

import org.apache.hive.basesql.cursor.CommonCursor;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.exception.AlreadyDeclaredException;

/**
 * Created by zhongdg1 on 2016/12/28.
 */
public class DeclareCursorStatement extends BaseStatement {
    private static final String STATEMENT_NAME = "_DECALRE_CURSOR_";
    private CommonCursor cursor;

    public DeclareCursorStatement() {
        super(STATEMENT_NAME);
    }

    public void setCursor(CommonCursor cursor) {
        this.cursor = cursor;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public int execute() throws Exception {

        if (null != findCursor(cursor.getName(), cursor.isGlobal())) {
            throw new AlreadyDeclaredException(cursor.getName());
        }

        cursor.setStatus(Cursor.CursorStatus.DECLARED);
        addCursor(cursor);

        return 0;
    }
}
