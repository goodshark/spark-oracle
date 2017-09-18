package org.apache.hive.plsql.cursor;

import org.apache.hive.basesql.cursor.CommonCursor;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.*;
import org.apache.hive.tsql.exception.NotDeclaredException;

import java.util.*;

/**
 * Created by dengrb1 on 7/17 0017.
 */
public class OracleFetchCursorStmt extends BaseStatement {
    private static final String STATEMENT_NAME = "_ORACLE_FETCH_CURSOR_";
    private String cursorName = "";
    private Map<String, Var> vars = new LinkedHashMap<>();
    private boolean bulkCollect = false;
    private OracleCursor cursor = null;
    private SparkResultSet resultSet = null;
    private boolean hasNoData = false;

    public OracleFetchCursorStmt() {
        super(STATEMENT_NAME);
    }

    public OracleFetchCursorStmt(String name) {
        super(STATEMENT_NAME);
        cursorName = name;
    }

    public void setBulkCollect() {
        bulkCollect = true;
    }

    public void addVarname(String name) {
        vars.put(name, null);
    }

    private void checkCursor() {
        cursor = (OracleCursor) findCursor(cursorName, false);
        if (null == cursor) {
            throw new NotDeclaredException(cursorName);
        }
        if (CommonCursor.CursorStatus.OPENING != cursor.getStatus()) {
            throw new RuntimeException("Cursor not opening # " + cursorName);
        }
    }

    private void bindVars() {
        for (String varName: vars.keySet()) {
            Var var = findVar(varName);
            if (var != null) {
                if (var.getDataType() == Var.DataType.REF_COMPOSITE && !var.isCompoundResolved())
                    bindComplexVar(var);
                if (var.getDataType() == Var.DataType.NESTED_TABLE) {
                    Var arrayTypeVar = var.getArrayVar(0);
                    if (!arrayTypeVar.isCompoundResolved())
                        bindComplexVar(arrayTypeVar);
                }
                vars.put(varName, var);
            } else
                throw new RuntimeException("Cursor fetch result into can not find variable: " + varName);
        }
    }

    private void fetchValues() throws Exception {
        Var[] varArray = new Var[vars.size()];
        varArray = vars.values().toArray(varArray);
        if (!bulkCollect) {
            resultSet.next();
            Row row = resultSet.fetchRow();
            if (varArray.length == 1 && varArray[0].getDataType() == Var.DataType.REF_COMPOSITE) {
                // fetch into complex Type
                Var complexVar = varArray[0];
                List<Column> schema = cursor.getSchema();
                if (schema.size() != row.getColumnSize())
                    throw new RuntimeException("schema size not equal col size");
                for (int i = 0; i < row.getColumnSize(); ++i) {
                    Column colType = schema.get(i);
                    String colName = colType.getColumnName().toUpperCase();
                    Object colVal = row.getColumnVal(i);
                    Var innerVar = complexVar.getInnerVar(colName);
                    if (innerVar == null)
                        throw new RuntimeException("col name " + colName + " not find in record var");
                    if (!innerVar.getDataType().toString().equalsIgnoreCase(colType.getDataType().toString()))
                        throw new RuntimeException("col name " + colName +
                                " Type " + colType.getDataType() + " not match record");
                    innerVar.setVarValue(colVal);
                }
            } else {
                // fetch into primitive types
                for (int i = 0; i < row.getColumnSize(); ++i) {
                    varArray[i].setVarValue(row.getColumnVal(i));
                }
            }
        } else {
            if (varArray.length != 1)
                throw new RuntimeException("cursor fetch into variable number wrong: " + varArray.length);
            Var tableVar = varArray[0];
            if (tableVar.getDataType() != Var.DataType.NESTED_TABLE)
                throw new RuntimeException("cursor fetch into variable is not table: " + tableVar.getVarName());
            List<Column> schema = cursor.getSchema();
            Var typeVar = tableVar.getArrayVar(0);
            while (resultSet.next()) {
                Row row = resultSet.fetchRow();
                Var rowVar = typeVar.clone();
                for (int i = 0; i < row.getColumnSize(); ++i) {
                    Column col = schema.get(i);
                    String colName = col.getColumnName().toUpperCase();
                    Object colVal = row.getColumnVal(i);
                    Var colVar = rowVar.getInnerVar(colName);
                    if (colVar == null)
                        throw new RuntimeException("col name " + colName + " not find in array var");
                    if (!colVar.getDataType().toString().equalsIgnoreCase(col.getDataType().toString()))
                        throw new RuntimeException("col name " + colName + " Type " + col.getDataType() + " not match array");
                    colVar.setVarValue(colVal);
                }
                tableVar.addNestedTableValue(rowVar);
                // TODO old implement
                tableVar.addArrayVar(rowVar);
            }
        }
    }

    private void bindComplexVar(Var var) {
        // resolve var Type
        List<Column> cols = cursor.getSchema();
        for (Column col: cols) {
            Var innerVar = new Var();
            String varName = col.getColumnName();
            innerVar.setVarName(varName);
            ColumnDataType columnDataType = col.getDataType();
            Var.DataType dataType = Var.DataType.valueOf(columnDataType.toString());
            innerVar.setDataType(dataType);
            var.addInnerVar(innerVar);
        }
        var.setCompoundResolved();
    }

    private void checkVars() throws Exception {
        resultSet = (SparkResultSet) cursor.getRs();
        Var[] varArray = new Var[vars.size()];
        varArray = vars.values().toArray(varArray);
        int varSize = varArray.length;
        if (resultSet == null || resultSet.getColumnSize() == 0) {
            hasNoData = true;
            return;
        }
        cursor.getSchema();
        /*if (varSize == 1 && (varArray[0].getDataType() == Var.DataType.REF_COMPOSITE ||
                varArray[0].getDataType() == Var.DataType.NESTED_TABLE)) {
            if (resultSet.getColumnSize() != varArray[0].getRecordSize())
                throw new RuntimeException("Cursor fetch results more than record size: " + cursorName);
        } else {
            if (resultSet == null || resultSet.getColumnSize() != vars.size())
                throw new RuntimeException("Cursor fetch results more than variables: " + cursorName);
        }*/
        if (varSize == 1) {
            if (varArray[0].getDataType() == Var.DataType.REF_COMPOSITE && resultSet.getColumnSize() != varArray[0].getRecordSize())
                throw new RuntimeException("Cursor fetch results more than record size: " + cursorName);
            if (varArray[0].getDataType() == Var.DataType.NESTED_TABLE) {
                Var typeVar = varArray[0].getArrayVar(0);
                if (typeVar.getRecordSize() != resultSet.getColumnSize())
                    throw new RuntimeException("Cursor fetch results more than array record size: " + cursorName);
            }
        } else {
            if (resultSet.getColumnSize() != vars.size())
                throw new RuntimeException("Cursor fetch results more than variables: " + cursorName);
        }
        // TODO checkCursor vars Type
    }

    @Override
    public int execute() throws Exception {
        checkCursor();
        bindVars();
        checkVars();
        if (hasNoData)
            return 0;
        fetchValues();
        return 0;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }
}
