package org.apache.hive.plsql.dml.fragment.explainFragment;

import org.apache.hive.plsql.dml.OracleSelectStatement;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.commonFragment.TableViewNameFragment;
import org.apache.hive.plsql.dml.fragment.delFragment.OracleDelStatement;
import org.apache.hive.plsql.dml.fragment.insertFragment.OracleInsertStatement;
import org.apache.hive.plsql.dml.fragment.updateFragment.OracleUpdateStatement;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/11.
 * <p>
 * explain_statement
 * : EXPLAIN PLAN (SET STATEMENT_ID '=' quoted_string)? (INTO tableview_name)?
 * FOR (select_statement | update_statement | delete_statement | insert_statement | merge_statement)
 * ;
 */
public class OracleExplainStatment extends SqlStatement {


    private String quotedStr;
    private TableViewNameFragment tableViewNameFragment;
    private OracleSelectStatement oracleSelectStatement;
    private OracleInsertStatement oracleInsertStatement;
    private OracleUpdateStatement oracleUpdateStatement;
    private OracleDelStatement oracleDelStatement;

    @Override
    public int execute() throws Exception {
        String sql = getOriginalSql();
        setRs(commitStatement(sql));
        return 0;
    }

    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append("EXPLAN ");
        ExecSession execSession = getExecSession();
        if (null != oracleSelectStatement) {
            sql.append(FragMentUtils.appendOriginalSql(oracleSelectStatement, execSession));
        }
        if (null != oracleInsertStatement) {
            sql.append(FragMentUtils.appendOriginalSql(oracleInsertStatement, execSession));
        }
        if (null != oracleUpdateStatement) {
            sql.append(FragMentUtils.appendOriginalSql(oracleUpdateStatement, execSession));
        }
        if (null != oracleDelStatement) {
            sql.append(FragMentUtils.appendOriginalSql(oracleDelStatement, execSession));
        }

        return sql.toString();
    }

    public String getQuotedStr() {
        return quotedStr;
    }

    public void setQuotedStr(String quotedStr) {
        this.quotedStr = quotedStr;
    }

    public TableViewNameFragment getTableViewNameFragment() {
        return tableViewNameFragment;
    }

    public void setTableViewNameFragment(TableViewNameFragment tableViewNameFragment) {
        this.tableViewNameFragment = tableViewNameFragment;
    }

    public OracleSelectStatement getOracleSelectStatement() {
        return oracleSelectStatement;
    }

    public void setOracleSelectStatement(OracleSelectStatement oracleSelectStatement) {
        this.oracleSelectStatement = oracleSelectStatement;
    }

    public OracleInsertStatement getOracleInsertStatement() {
        return oracleInsertStatement;
    }

    public void setOracleInsertStatement(OracleInsertStatement oracleInsertStatement) {
        this.oracleInsertStatement = oracleInsertStatement;
    }

    public OracleUpdateStatement getOracleUpdateStatement() {
        return oracleUpdateStatement;
    }

    public void setOracleUpdateStatement(OracleUpdateStatement oracleUpdateStatement) {
        this.oracleUpdateStatement = oracleUpdateStatement;
    }

    public OracleDelStatement getOracleDelStatement() {
        return oracleDelStatement;
    }

    public void setOracleDelStatement(OracleDelStatement oracleDelStatement) {
        this.oracleDelStatement = oracleDelStatement;
    }
}
