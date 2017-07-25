package org.apache.hive.tsql.cursor;

import org.apache.hive.basesql.cursor.CommonCursor;
import org.apache.hive.tsql.common.TreeNode;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhongdg1 on 2016/12/28.
 */
public class Cursor extends CommonCursor {
    public enum DataMode {STATIC, KEYSET, DYNAMIC, FAST_FORWARD}

    public enum ConsistencyMode {READ_ONLY, SCROLL_LOCKS, OPTIMISTIC}

    private boolean isInsensitive = false;
    private boolean isScoll = false;
    private boolean typeWarning = false;
    private boolean isUpdatable = false;
    private DataMode dataMode = DataMode.STATIC;
    private ConsistencyMode consistencyMode = ConsistencyMode.READ_ONLY;
    private Set<String> updatableColumns = new HashSet<String>();
    public Cursor() {
    }

    public Cursor(String name) {
        super(name.toUpperCase());
    }

    public boolean isInsensitive() {
        return isInsensitive;
    }

    public boolean isScoll() {
        return isScoll;
    }

    public boolean isTypeWarning() {
        return typeWarning;
    }

    public boolean isUpdatable() {
        return isUpdatable;
    }

    public DataMode getDataMode() {
        return dataMode;
    }

    public ConsistencyMode getConsistencyMode() {
        return consistencyMode;
    }

    public void addUpdatableColumns(Collection<String> columns) {
        updatableColumns.addAll(columns);
    }

    public void setTypeWarning(boolean typeWarning) {
        this.typeWarning = typeWarning;
    }

    public void setUpdatable(boolean updatable) {
        isUpdatable = updatable;
    }

    public void setDataMode(DataMode dataMode) {
        this.dataMode = dataMode;
    }

    public void setConsistencyMode(ConsistencyMode consistencyMode) {
        this.consistencyMode = consistencyMode;
    }

    public void setInsensitive(boolean insensitive) {
        isInsensitive = insensitive;
    }

    public void setScoll(boolean scoll) {
        isScoll = scoll;
    }
}

