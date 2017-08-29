package org.apache.hive.plsql.cfl;

import org.apache.hive.plsql.cursor.OracleCloseCursorStmt;
import org.apache.hive.plsql.cursor.OracleCursor;
import org.apache.hive.plsql.cursor.OracleFetchCursorStmt;
import org.apache.hive.plsql.cursor.OracleOpenCursorStmt;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.Column;
import org.apache.hive.tsql.common.ColumnDataType;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.node.LogicNode;

import java.util.List;

/**
 * Created by dengrb1 on 8/21 0021.
 */
public class LoopCursorConditionStmt extends LogicNode {
    private String indexName;
    protected String cursorName = "ANONYMOUS_CURSOR";
    protected OracleCursor cursor;
    private Var indexVar;
    private OracleFetchCursorStmt fetchCursorStmt;

    public void setCursorName(String name) {
        cursorName = name;
    }

    public void setIndexName(String name) {
        indexName = name;
    }

    @Override
    public void initIndex() throws Exception {
        /*// find cursor
        cursor = (OracleCursor) findCursor(cursorName);
        if (cursor == null)
            throw new Exception("find cursor in cursor loop failed: " + cursorName);
        // open cursor
        OracleOpenCursorStmt openCursorStmt = new OracleOpenCursorStmt(cursorName, false);
        openCursorStmt.setExecSession(getExecSession());
        openCursorStmt.execute();
        // create complex var with cursor & add var into scope
        indexVar = new Var();
        indexVar.setVarName(indexName);
        indexVar.setDataType(Var.DataType.COMPLEX);

        List<Column> cols = cursor.getSchema();
        for (Column col: cols) {
            Var innerVar = new Var();
            String varName = col.getColumnName();
            innerVar.setVarName(varName);
            ColumnDataType columnDataType = col.getDataType();
            Var.DataType dataType = Var.DataType.valueOf(columnDataType.toString());
            innerVar.setDataType(dataType);
            indexVar.addInnerVar(innerVar);
        }
        indexVar.setCompoundResolved();

        addVar(indexVar);

        fetchCursorStmt = new OracleFetchCursorStmt(cursorName);
        fetchCursorStmt.setExecSession(getExecSession());
        fetchCursorStmt.addVarname(indexName);*/
        getCursor();
        openCursor();
        bindVar();
    }

    protected void getCursor() throws Exception {
        cursor = (OracleCursor) findCursor(cursorName);
        if (cursor == null)
            throw new Exception("find cursor in cursor loop failed: " + cursorName);
    }

    private void openCursor() throws Exception {
        OracleOpenCursorStmt openCursorStmt = new OracleOpenCursorStmt(cursorName, false);
        openCursorStmt.setExecSession(getExecSession());
        openCursorStmt.execute();
    }

    private void bindVar() throws Exception {
        indexVar = new Var();
        indexVar.setVarName(indexName);
        indexVar.setDataType(Var.DataType.COMPLEX);

        List<Column> cols = cursor.getSchema();
        for (Column col: cols) {
            Var innerVar = new Var();
            String varName = col.getColumnName();
            innerVar.setVarName(varName);
            ColumnDataType columnDataType = col.getDataType();
            Var.DataType dataType = Var.DataType.valueOf(columnDataType.toString());
            innerVar.setDataType(dataType);
            indexVar.addInnerVar(innerVar);
        }
        indexVar.setCompoundResolved();

        addVar(indexVar);

        fetchCursorStmt = new OracleFetchCursorStmt(cursorName);
        fetchCursorStmt.setExecSession(getExecSession());
        fetchCursorStmt.addVarname(indexName);
    }

    @Override
    public int execute() throws Exception {
        // if can not fetch more result from cursor, close cursor
        fetchCursorStmt.execute();
        boolean res = cursor.hasMoreRows();
        setBool(res);
        setRs(new SparkResultSet().addRow(new Object[] {new Var("bool result", res, Var.DataType.BOOLEAN)}));
        if (!res) {
            OracleCloseCursorStmt closeCursorStmt = new OracleCloseCursorStmt(cursorName, false);
            closeCursorStmt.setExecSession(getExecSession());
            closeCursorStmt.execute();
        }
        return 0;
    }
}
