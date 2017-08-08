package org.apache.hive.plsql.ddl.fragment.createTableFragment;

import org.apache.hive.plsql.ddl.commonFragment.CrudTableFragment;
import org.apache.hive.plsql.dml.commonFragment.ColumnNameFragment;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.commonFragment.TableViewNameFragment;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.common.TmpTableNameUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/28.
 * create_table
 * : CREATE  (GLOBAL TEMPORARY)? TABLE tableview_name
 * '(' column_name type_spec column_constraint?
 * (',' column_name type_spec column_constraint?)* ')' crud_table?
 * table_space? storage?
 * tmp_tb_comments?
 * ;
 */
public class OracleCreateTableStatement extends SqlStatement {
    private boolean tempTable;
    private TableViewNameFragment tableViewNameFragment;
    private List<ColumnNameFragment> columns = new ArrayList<>();
    private List<Var.DataType> columnType = new ArrayList<>();
    private CrudTableFragment crudTableFragment;

    private String tableName;
    private String tableAliasName;


    @Override
    public int execute() throws Exception {
        String sql = getOriginalSql();
        setRs(commitStatement(sql));
       /* TmpTableNameUtils tableNameUtils = new TmpTableNameUtils();
        HashMap<Integer, HashMap<String, String>> sparkSessonTableMap = getExecSession().getSparkSession().getSqlServerTable();
        if (tableNameUtils.checkIsTmpTable(tableName)) {
            addTmpTable(tableName, tableAliasName);
            addTableToSparkSeesion(tableName, tableAliasName, sparkSessonTableMap, 2);

        }*/
        return 0;
    }

    private void addTableToSparkSeesion(String tableName, String tableAliasName, HashMap<Integer, HashMap<String, String>> map, int key) {
        if (null != map.get(key)) {
            map.get(key).put(tableName, tableAliasName);
        } else {
            HashMap<String, String> tb = new HashMap<>();
            tb.put(tableName, tableAliasName);
            map.put(key, tb);
        }
    }

    @Override
    public String getFinalSql() throws Exception {
        return getOriginalSql();
    }

    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append("CREATE TABLE ");
        ExecSession execSession = getExecSession();
        tableName = FragMentUtils.appendOriginalSql(tableViewNameFragment, execSession).trim();
        if (tempTable) {
            tableName = "#" + tableName;
        }
        TmpTableNameUtils tableNameUtils = new TmpTableNameUtils();
        tableAliasName = tableNameUtils.createTableName(tableName);
        sql.append(tableAliasName);
        sql.append("(");
        StringBuffer columnSql = new StringBuffer();
        for (int i = 0; i < columns.size(); i++) {
            columnSql.append(FragMentUtils.appendOriginalSql(columns.get(i), execSession));
            columnSql.append(" ");
            columnSql.append(columnType.get(i).toString());
            columnSql.append(" ,");
        }
        sql.append(columnSql.substring(0, columnSql.length() - 1));
        sql.append(")");
        if (null != crudTableFragment) {
            sql.append(FragMentUtils.appendOriginalSql(crudTableFragment, execSession));
        }
        return sql.toString();
    }

    public void addColumnName(ColumnNameFragment columnNameFragment) {
        this.columns.add(columnNameFragment);
    }

    public void addColumnType(Var.DataType d) {
        this.columnType.add(d);
    }

    public boolean isTempTable() {
        return tempTable;
    }

    public void setTempTable(boolean tempTable) {
        this.tempTable = tempTable;
    }

    public TableViewNameFragment getTableViewNameFragment() {
        return tableViewNameFragment;
    }

    public void setTableViewNameFragment(TableViewNameFragment tableViewNameFragment) {
        this.tableViewNameFragment = tableViewNameFragment;
    }

    public CrudTableFragment getCrudTableFragment() {
        return crudTableFragment;
    }

    public void setCrudTableFragment(CrudTableFragment crudTableFragment) {
        this.crudTableFragment = crudTableFragment;
    }
}
