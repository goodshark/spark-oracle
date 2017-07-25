package org.apache.hive.plsql.cursor;

import org.apache.hive.basesql.cursor.CommonCursor;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.Row;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.exception.NotDeclaredException;

import java.util.*;

/**
 * Created by dengrb1 on 7/17 0017.
 */
public class OracleFetchCursorStmt extends BaseStatement {
    private static final String STATEMENT_NAME = "_ORACLE_FETCH_CURSOR_";
    private String cursorName = "";
    private Map<String, Var> vars = new TreeMap<>();
    private boolean bulkCollect = false;
    private OracleCursor cursor = null;
    private SparkResultSet resultSet = null;

    public OracleFetchCursorStmt() {
        super(STATEMENT_NAME);
    }

    public OracleFetchCursorStmt(String name) {
        super(STATEMENT_NAME);
        cursorName = name;
    }

    public void addVarname(String name) {
        vars.put(name, null);
    }

    private void check() {
        cursor = (OracleCursor) findCursor(cursorName, false);
        if (null == cursor) {
            throw new NotDeclaredException(cursorName);
        }
        if (CommonCursor.CursorStatus.OPENING != cursor.getStatus()) {
            throw new RuntimeException("Cursor not opening # " + cursorName);
        }
        resultSet = (SparkResultSet) cursor.getRs();
        if (resultSet == null || resultSet.getColumnSize() != vars.size())
            throw new RuntimeException("Cursor fetch results more than variables: " + cursorName);
        // TODO check vars type
    }

    private void bindVars() {
        for (String varName: vars.keySet()) {
            Var var = findVar(varName);
            if (var != null)
                vars.put(varName, var);
            else
                throw new RuntimeException("Cursor fetch result into can not find variable: " + varName);
        }
    }

    private void fetchValues() throws Exception {
        if (!bulkCollect) {
            resultSet.next();
            Row row = resultSet.fetchRow();
            Var[] varArray = new Var[vars.size()];
            varArray = vars.values().toArray(varArray);
            for (int i = 0; i < row.getColumnSize(); ++i) {
                varArray[i].setVarValue(row.getColumnVal(i));
            }
        } else {
            // TODO
        }
    }

    @Override
    public int execute() throws Exception {
        bindVars();
        check();
        fetchValues();
        return 0;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }
}
