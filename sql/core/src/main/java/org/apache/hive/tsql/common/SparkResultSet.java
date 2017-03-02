package hive.tsql.common;

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
        for (org.apache.spark.sql.Row r : list) {
            Row row = new Row(filedSize);
            Object values[] = new Object[filedSize];
            for (int i = 0; i < filedSize; i++) {
                values[i] = r.get(i);
            }
            row.setValues(values);
            addRow(row);
        }
        StructField structField[] = dataset.schema().fields();
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
            return false;
        }
        if (isFirstFetch) {
            isFirstFetch = false;
            return true;
        }
        index++;
        return true;
    }


    @Override
    public boolean previous() throws SQLException {
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
        index = row >= 0 ? row - 1 : currentSize + row;
        if (index >= currentSize || index < 0) {
            return false;
        }
        return true;
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        if (isFirstFetch && rows < 1) {
            return false;
        }


        int targetIndex = index + rows;
        if (isFirstFetch) {
            targetIndex -= 1;
            isFirstFetch = false;
        }
        if (currentSize == 0) {
            return false;
        }
        if (0 > targetIndex || currentSize <= targetIndex) {
            return true;
        }

        index = targetIndex;
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

}
