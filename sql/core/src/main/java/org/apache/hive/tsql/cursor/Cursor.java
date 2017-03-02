package hive.tsql.cursor;

import org.apache.hive.tsql.common.TreeNode;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by zhongdg1 on 2016/12/28.
 */
public class Cursor {
    public enum DataMode {STATIC, KEYSET, DYNAMIC, FAST_FORWARD}

    public enum ConsistencyMode {READ_ONLY, SCROLL_LOCKS, OPTIMISTIC}

    public enum CursorStatus {
        DECLARED(1), OPENING(2), FETCHING(3),CLOSED(4), DEALLOCATED(5);

        CursorStatus(int code) {
        }
    }

    private String name;
    private TreeNode treeNode; //select statement
    private boolean isInsensitive = false;
    private boolean isScoll = false;
    private boolean isGlobal = false;
    private boolean typeWarning = false;
    private boolean isUpdatable = false;
    private DataMode dataMode = DataMode.STATIC;
    private ConsistencyMode consistencyMode = ConsistencyMode.READ_ONLY;
    private Set<String> updatableColumns = new HashSet<String>();
    private CursorStatus status = CursorStatus.DECLARED;
    private ResultSet rs;
    public Cursor() {
    }

    public Cursor(String name) {
        this.name = name.toUpperCase();
    }

    public String getName() {
        return name.toUpperCase();
    }

    public boolean isInsensitive() {
        return isInsensitive;
    }

    public boolean isScoll() {
        return isScoll;
    }

    public boolean isGlobal() {
        return isGlobal;
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

    public CursorStatus getStatus() {
        return status;
    }

    public void setStatus(CursorStatus status) {
        this.status = status;
    }

    public void addUpdatableColumns(Collection<String> columns) {
        updatableColumns.addAll(columns);
    }

    public void setGlobal(boolean global) {
        isGlobal = global;
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

    public void setName(String name) {
        this.name = name.toUpperCase();
    }

    public void setTreeNode(TreeNode treeNode) {
        this.treeNode = treeNode;
    }

    public TreeNode getTreeNode() {
        return treeNode;
    }

    public ResultSet getRs() {
        return rs;
    }

    public void setRs(ResultSet rs) {
        this.rs = rs;
    }
}

