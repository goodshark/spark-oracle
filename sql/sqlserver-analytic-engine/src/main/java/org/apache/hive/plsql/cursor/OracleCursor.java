package org.apache.hive.plsql.cursor;

import org.apache.hive.basesql.cursor.CommonCursor;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.Column;
import org.apache.hive.tsql.common.SparkResultSet;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 7/13 0013.
 */
public class OracleCursor extends CommonCursor {
    private int curRowNum = -1;
    private List<Column> schema = new ArrayList<>();
    // TODO cursor with parameters
    private List<Var> vars = new ArrayList<>();

    public OracleCursor() {
    }

    public OracleCursor(String name) {
        super(name);
    }

    @Override
    public int execute() throws Exception {
        return 0;
    }

    public int getCurrentRowCount() {
        SparkResultSet rs = (SparkResultSet) getRs();
        curRowNum = rs.getCurrentRowNumber();
        return curRowNum;
    }

    public boolean hasMoreRows() {
        SparkResultSet rs = (SparkResultSet) getRs();
        return rs.hasMoreRows();
    }

    public void setSchema(List<Column> s) {
        schema = s;
    }

    public List<Column> getSchema() {
        return schema;
    }

    public void addParameter(Var v) {
        vars.add(v);
    }

    public int getParameterSize() {
        return vars.size();
    }

    public List<Var> getCursorParaCopys() {
        return new ArrayList<>(vars);
    }
}
