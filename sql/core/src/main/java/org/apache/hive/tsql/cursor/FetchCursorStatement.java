package hive.tsql.cursor;

import org.apache.hive.tsql.arg.SystemVName;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.Row;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.exception.CursorOperationException;
import org.apache.hive.tsql.exception.NotDeclaredException;
import org.apache.hive.tsql.util.StrUtils;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhongdg1 on 2016/12/28.
 */
public class FetchCursorStatement extends BaseStatement {
    private static final String STATEMENT_NAME = "_FETCH_CURSOR_";

    public enum FetchDirection {NEXT, PRIOR, FIRST, LAST, ABSOLUTE, RELATIVE}

    public FetchCursorStatement() {
        super(STATEMENT_NAME);
    }

    private FetchDirection direction = FetchDirection.NEXT;
    private TreeNode expr; //n 通过表达式计算出来的正/负整数
    private boolean isGlobal = false;
    private String cursorName;
    private List<String> intoVarNames = new ArrayList<>(); //into @x,@y


    public TreeNode getExpr() {
        return expr;
    }

    public void setExpr(TreeNode expr) {
        this.expr = expr;
    }

    public void setGlobal(boolean global) {
        isGlobal = global;
    }

    public void setCursorName(String cursorName) {
        this.cursorName = cursorName.trim().toUpperCase();
    }

    public void addIntoVarName(String varName) {
        intoVarNames.add(varName);
    }

    public void setDirection(FetchDirection direction) {
        this.direction = direction;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public int execute() throws Exception {
        Cursor cursor = isGlobal ? findCursor(cursorName, true) : findCursor(cursorName);
        if (null == cursor) {
            throw new NotDeclaredException(cursorName);
        }
        if (Cursor.CursorStatus.OPENING != cursor.getStatus()) {
            throw new RuntimeException("Cursor not opening # " + cursorName);
        }
        SparkResultSet rs = (SparkResultSet) cursor.getRs();

        check(cursor);

        if (intoVarNames.isEmpty()) {
            if (movePointer(rs)) {
                org.apache.spark.sql.Row r = rs.getDataset().first();
                StringBuffer sb = new StringBuffer();
                sb.append("SELECT ");
                for (int i = 0; i < r.size(); i++) {
                    if (i > 0) {
                        sb.append(",");
                    }
                    sb.append(r.get(i) == null ? null : StrUtils.addQuot(r.get(i).toString()));
                }
                commitStatement(sb.toString());
            }
            return 0;
        }


        List<Var> intoVars = new ArrayList<>();
        for (String varName : intoVarNames) {
            Var var = findVar(varName);
            if (null == var) {
                throw new NotDeclaredException(varName);
            }
            var.setVarValue(null);
            intoVars.add(var);
        }

        if (!movePointer(rs)) {
            return 1;
        }

        Row row = rs.fetchRow();
        for (int i = 0; i < intoVars.size(); i++) {
            Var v = intoVars.get(i);
            v.setVarValue(row.getColumnVal(i));
        }

        return 0;
    }

    private void check(Cursor cursor) throws RuntimeException {
        if ((!cursor.isScoll() || cursor.isInsensitive()) && direction != FetchDirection.NEXT) {
            throw new CursorOperationException("INSENSITIVE/NONSCOLL only supported NEXT for fetch data.");
        }

        if (cursor.getDataMode() == Cursor.DataMode.DYNAMIC && direction == FetchDirection.ABSOLUTE) {
            throw new CursorOperationException("DYNAMIC unsupported ABSOLUTE.");
        }
    }

    public boolean movePointer(ResultSet rs) throws Exception {
        boolean flag = false;
        switch (direction) {
            case NEXT:
                flag = rs.next();
                break;
            case PRIOR:
                flag = rs.previous();
                break;
            case FIRST:
                flag = rs.first();
                break;
            case LAST:
                flag = rs.last();
                break;
            case ABSOLUTE:
                expr.setExecSession(getExecSession());
                this.expr.execute();
                int rows = expr.getExpressionValue().getInt();
                flag = rs.absolute(rows);
                break;
            case RELATIVE:
                expr.setExecSession(getExecSession());
                this.expr.execute();
                int rrows = expr.getExpressionValue().getInt();
                flag = rs.relative(rrows);
                break;
            default:
                break;
        }
        boolean fetchStatus = flag ? updateSys(SystemVName.FETCH_STATUS, 0) : updateSys(SystemVName.FETCH_STATUS, -1);
        return flag;
    }
}
