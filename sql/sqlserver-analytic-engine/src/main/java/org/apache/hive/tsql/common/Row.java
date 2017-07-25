package org.apache.hive.tsql.common;

/**
 * Created by zhongdg1 on 2016/12/30.
 */
public class Row {
    private int columnSize = 0;

    private Object[] values;

    public Row(int columnSize) {
        this.columnSize = columnSize;
        values = new Object[columnSize];
    }

    public Object getColumnVal(int index) {
        return values[index];
    }

    public void updateColumnVal(int index, Object val) {
        values[index] = val;
    }

    public int getColumnSize() {
        return columnSize;
    }

    public Row setValues(Object[] values) {
        this.values = values;
        return this;
    }
}
