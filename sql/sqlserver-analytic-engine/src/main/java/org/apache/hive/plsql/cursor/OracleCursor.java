package org.apache.hive.plsql.cursor;

import org.apache.hive.basesql.cursor.CommonCursor;

/**
 * Created by dengrb1 on 7/13 0013.
 */
public class OracleCursor extends CommonCursor {
    // TODO mark cursor row/hasMoreResult status
    private int curRowNum = -1;

    public OracleCursor() {
    }

    public OracleCursor(String name) {
        super(name);
    }

    public boolean hasMoreRows() {
        return false;
    }
}
