package org.apache.hive.plsql.dml.fragment.mergeFragment;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.commonFragment.TableAliasFragment;
import org.apache.hive.plsql.dml.commonFragment.TableViewNameFragment;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;
import org.apache.spark.sql.catalyst.expressions.IfNull;

import javax.swing.*;
import java.util.List;

/**
 * Created by wangsm9 on 2017/8/4.
 * merge_statement
 * : MERGE INTO tableview_name table_alias? USING selected_tableview ON '(' condition ')'
 * (merge_update_clause merge_insert_clause? | merge_insert_clause merge_update_clause?)?
 * error_logging_clause?
 * ;
 */
public class OracleMergeIntoStatment extends SqlStatement {
    private TableViewNameFragment tableViewNameFragment;
    private TableAliasFragment tableAliasFragment;
    private ExpressionStatement expressionStatement;

    private MergerUpdateClauseFm mergerUpdateClauseFm;
    private MergerInsertClauseFm mergerInsertClauseFm;
    private SelectTableViewFragment selectTableViewFragment;


    private String tableName;
    private String tableNameAlias;
    private String conditionSql;

    private String updateSql;
    private String insertIntoSql;
    private String delSql;


    @Override
    public String getFinalSql() throws Exception {


        ExecSession execSession = getExecSession();
        tableName = FragMentUtils.appendOriginalSql(tableViewNameFragment, execSession);
        if (null != tableAliasFragment) {
            tableNameAlias = FragMentUtils.appendOriginalSql(tableAliasFragment, execSession);
        }
        conditionSql = FragMentUtils.appendOriginalSql(expressionStatement, execSession);

        //WHEN MATCHED THEN UPDATE SET merge_element (',' merge_element)* where_clause? merge_update_delete_part?
        if (null != mergerUpdateClauseFm) {
            StringBuffer updateSqlSb = new StringBuffer();
            List<MergerElementFragement> mergerElems = mergerUpdateClauseFm.getMergerElementFragements();
            updateSqlSb.append("UPDATE ");
            updateSqlSb.append(tableName);
            updateSqlSb.append(" ");
            appendTableAliasName(updateSqlSb);
            updateSqlSb.append(" SET ");
            updateSqlSb.append(FragMentUtils.appendOriginalSql(mergerElems, execSession));
            updateSqlSb.append(" ");
            updateSqlSb.append(" join ");
            updateSqlSb.append(FragMentUtils.appendOriginalSql(selectTableViewFragment, execSession));
            updateSqlSb.append(" on ");
            updateSqlSb.append(conditionSql);
            if (null != mergerUpdateClauseFm.getWhereClauseFragment()) {
                updateSqlSb.append(FragMentUtils.appendOriginalSql(mergerUpdateClauseFm.getWhereClauseFragment(), execSession));
            }
            updateSql = updateSqlSb.toString();
            if (null != mergerUpdateClauseFm.getMergerUpdateDelFragment()) {
                StringBuffer delSqlSb = new StringBuffer();
                delSqlSb.append("DELETE ");
                delSqlSb.append(tableName);
                appendTableAliasName(delSqlSb);
                delSqlSb.append(" join ");
                delSqlSb.append(FragMentUtils.appendOriginalSql(selectTableViewFragment, execSession));
                delSqlSb.append(" on ");
                delSqlSb.append(conditionSql);
                delSqlSb.append(FragMentUtils.appendOriginalSql(mergerUpdateClauseFm.getMergerUpdateDelFragment().getWhereClauseFragment(), execSession));
                delSql = delSqlSb.toString();
            }
        }


        //: WHEN NOT MATCHED THEN INSERT ('(' column_name (',' column_name)* ')')? VALUES expression_list where_clause?
        if (null != mergerInsertClauseFm) {
            StringBuffer insertSqlSb = new StringBuffer();
            insertSqlSb.append("INSERT INTO ");
            insertSqlSb.append(tableName);
            appendTableAliasName(insertSqlSb);
            insertSqlSb.append(" (");
            insertSqlSb.append(FragMentUtils.appendOriginalSql(mergerInsertClauseFm.getColumnNames(), execSession));
            insertSqlSb.append(")");
            insertSqlSb.append(" VALUES ");
            insertSqlSb.append(" (");
            insertSqlSb.append(FragMentUtils.appendOriginalSql(mergerInsertClauseFm.getExpressionListFragment(), execSession));
            insertSqlSb.append(" )");
            insertSqlSb.append(" join ");
            insertSqlSb.append(FragMentUtils.appendOriginalSql(selectTableViewFragment, execSession));

            insertSqlSb.append(" on !(");
            insertSqlSb.append(conditionSql);
            insertSqlSb.append(")");
            insertSqlSb.append(FragMentUtils.appendOriginalSql(mergerInsertClauseFm.getWhereClauseFragment(), execSession));
            insertIntoSql = insertSqlSb.toString();
        }


        return "";

    }

    private void appendTableAliasName(StringBuffer updateSqlSb) {
        if (StringUtils.isNotBlank(tableNameAlias)) {
            updateSqlSb.append(tableNameAlias);
            updateSqlSb.append(" ");
        }
    }


    @Override
    public int execute() throws Exception {
        getFinalSql();
        if (StringUtils.isNotBlank(delSql)) {
            commitStatement(delSql);
        }
        if (StringUtils.isNotBlank(insertIntoSql)) {
            commitStatement(insertIntoSql);
        }
        if (StringUtils.isNotBlank(updateSql)) {
            commitStatement(updateSql);
        }


        return 0;
    }

    public MergerUpdateClauseFm getMergerUpdateClauseFm() {
        return mergerUpdateClauseFm;
    }

    public MergerInsertClauseFm getMergerInsertClauseFm() {
        return mergerInsertClauseFm;
    }

    public void setMergerUpdateClauseFm(MergerUpdateClauseFm mergerUpdateClauseFm) {
        this.mergerUpdateClauseFm = mergerUpdateClauseFm;
    }

    public void setMergerInsertClauseFm(MergerInsertClauseFm mergerInsertClauseFm) {
        this.mergerInsertClauseFm = mergerInsertClauseFm;
    }

    public TableViewNameFragment getTableViewNameFragment() {
        return tableViewNameFragment;
    }

    public void setTableViewNameFragment(TableViewNameFragment tableViewNameFragment) {
        this.tableViewNameFragment = tableViewNameFragment;
    }

    public TableAliasFragment getTableAliasFragment() {
        return tableAliasFragment;
    }

    public void setTableAliasFragment(TableAliasFragment tableAliasFragment) {
        this.tableAliasFragment = tableAliasFragment;
    }

    public ExpressionStatement getExpressionStatement() {
        return expressionStatement;
    }

    public void setExpressionStatement(ExpressionStatement expressionStatement) {
        this.expressionStatement = expressionStatement;
    }

    public SelectTableViewFragment getSelectTableViewFragment() {
        return selectTableViewFragment;
    }

    public void setSelectTableViewFragment(SelectTableViewFragment selectTableViewFragment) {
        this.selectTableViewFragment = selectTableViewFragment;
    }
}
