package org.apache.hive.tsql.common;

import org.apache.hive.tsql.arg.Var;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhongdg1 on 2016/12/20.
 */
public class SparkResultSet extends BaseResultSet {
    private static final Logger LOG = LoggerFactory.getLogger(SparkResultSet.class);
    private List<Column> columns = new ArrayList<>();
    private List<Row> data = new ArrayList<>();
    private Dataset<org.apache.spark.sql.Row> dataset;
    private boolean isFirstFetch = true;

    public SparkResultSet() {
    }

    public SparkResultSet(Dataset dataset) {
        this.dataset = dataset;
        List<org.apache.spark.sql.Row> list = dataset.collectAsList();
        int filedSize = dataset.schema().fieldNames().length;
        StructField structField[] = dataset.schema().fields();
        for (org.apache.spark.sql.Row r : list) {
            Row row = new Row(filedSize);
            Object values[] = new Object[filedSize];
            for (int i = 0; i < filedSize; i++) {
                Var var = new Var();
                var.setDataType(Var.DataType.valueOf(structField[i].dataType().typeName().toUpperCase().replaceAll("\\(.*\\)", "")));
                var.setVarValue(r.get(i));
                values[i] = var;
            }
            row.setValues(values);
            addRow(row);
        }
        for (StructField s : structField) {
            //TODO 实现获取列的别名
            String asName = "";
            ColumnDataType columnDataType = ColumnDataType.valueOf(s.dataType().typeName().toUpperCase().replaceAll("\\(.*\\)", ""));
            Column column = new Column(s.name(), asName, columnDataType);
            columns.add(column);
        }
    }


    private int currentSize = 0;
    private int index = -1;

    public void addColumn(Column column) {
        columns.add(column);
    }

    public int getColumnSize() {
        return columns.size();
    }


    public List<Column> getColumns() {
        return columns;
    }

    public SparkResultSet addColumns(List<Column> columns) {
        if (null != columns) {
            columns.addAll(columns);
        }

        return this;
    }

    public SparkResultSet addRow(Row row) {
        data.add(row);
        currentSize++;
        if (-1 == index) {
            index = 0;
        }
        return this;
    }

    public SparkResultSet addRow(Object[] values) {
        data.add(new Row(values.length).setValues(values));
        currentSize++;
        if (-1 == index) {
            index = 0;
        }
        return this;
    }


    @Override
    public int getRow() throws SQLException {
        return currentSize;
    }

    @Override
    public boolean next() throws SQLException {
        if (!isFirstFetch && (0 == currentSize || index == currentSize - 1)) {
            alreadyGetLast = true;
            return false;
        }
        if (isFirstFetch) {
            isFirstFetch = false;
            currentRowNumber++;
            return true;
        }
        index++;
        currentRowNumber++;
        return true;
    }


    @Override
    public boolean previous() throws SQLException {
        if (!isFirstFetch && index == 0) {
            return true;
        }
        if (0 == currentSize || index < 1) {
            return false;
        }
        index--;
        return true;
    }

    @Override
    public boolean first() throws SQLException {

        if (0 == currentSize) {
            return false;
        }
        isFirstFetch = false;
        index = 0;
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        if (0 == currentSize) {
            return false;
        }
        index = currentSize - 1;
        return true;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        int newIndex = row >= 0 ? row - 1 : currentSize + row;

        if (newIndex >= currentSize || newIndex < 0) {
            return !isFirstFetch;
        }
        isFirstFetch = false;
        index = newIndex;
        return true;
    }

    private int lastIndex = 0;

    @Override
    public boolean relative(int rows) throws SQLException {
        if (isFirstFetch && rows < 1) {
            return false;
        }
        if (currentSize == 0) {
            return false;
        }

        lastIndex = lastIndex + rows;

        if (isFirstFetch) {
            isFirstFetch = false;
            lastIndex--;
        }
        if (lastIndex >= currentSize) {
            lastIndex = currentSize;
            return true;
        }

        if (lastIndex < 0) {
            lastIndex = -1;
            return true;
        }

//        if (lastIndexBeyonded && rows < 0) {
//            index = currentSize + rows;
//            return true;
//        }
//
//        if (0 > targetIndex || currentSize <= targetIndex) {
//            lastIndexBeyonded = true;
//            return true;
//        }

        index = lastIndex;
        return true;
    }

    public Row fetchRow() throws SQLException {
        if (currentSize == 0 || index < 0 || index >= currentSize) {
            return null;
        }
        return data.get(index);
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return fetchRow().getColumnVal(columnIndex);
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Object obj = getObject(columnIndex);
        return null == obj ? null : Integer.valueOf(obj.toString());
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object obj = getObject(columnIndex);
        return null == obj ? null : String.valueOf(obj.toString());
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Object obj = getObject(columnIndex);
        return null == obj ? null : Date.valueOf(obj.toString());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object obj = getObject(columnIndex);
        return null == obj ? null : Double.valueOf(obj.toString());
    }

    public List<String> getFiledName() {
        List<String> list = new ArrayList<>();
        for (Column c : columns) {
            list.add(c.getColumnName());
        }
        return list;
    }

    public int getFiledCount() {
        return columns.size();
    }

    public List<ColumnDataType> getFileType() {
        List<ColumnDataType> list = new ArrayList<>();
        for (Column c : columns) {
            list.add(c.getDataType());
        }
        return list;
    }

    public long getRsCount() {
        return dataset.count();
    }

    public Dataset<org.apache.spark.sql.Row> getDataset() {
        return dataset;
    }

    public int getIndex() {
        return index;
    }

    int currentRowNumber = 0;
    public int getCurrentRowNumber() {
        return currentRowNumber;
    }

    boolean alreadyGetLast = false;
    public boolean hasMoreRows() {
        if (data.size() == 0)
            return false;
        return !alreadyGetLast;
    }
}
