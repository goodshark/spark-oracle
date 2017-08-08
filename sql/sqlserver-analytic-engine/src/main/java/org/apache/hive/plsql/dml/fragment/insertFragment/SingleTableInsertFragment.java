package org.apache.hive.plsql.dml.fragment.insertFragment;

import org.apache.hive.plsql.dml.OracleSelectStatement;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.fragment.updateFragment.StaticReturningClauseFm;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/11.
 * <p>
 * single_table_insert
 * : insert_into_clause (values_clause static_returning_clause? | select_statement) error_logging_clause?
 * ;
 */
public class SingleTableInsertFragment extends SqlStatement {
    private InsertIntoClauseFm insertIntoClauseFm;
    private ValuesClauseFragment valuesClauseFragment;
    private StaticReturningClauseFm staticReturningClauseFm;
    private OracleSelectStatement oracleSelectStatement;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        ExecSession execSession = getExecSession();
        sql.append(FragMentUtils.appendOriginalSql(insertIntoClauseFm, execSession));

        if (null != valuesClauseFragment) {
            sql.append("(");
            sql.append(FragMentUtils.appendOriginalSql(valuesClauseFragment, execSession));
            sql.append(")");
        }
        if (null != staticReturningClauseFm) {
            sql.append(FragMentUtils.appendOriginalSql(staticReturningClauseFm, execSession));
        }
        if (null != oracleSelectStatement) {
            sql.append(FragMentUtils.appendOriginalSql(oracleSelectStatement, execSession));
        }

        return sql.toString();
    }

    public InsertIntoClauseFm getInsertIntoClauseFm() {
        return insertIntoClauseFm;
    }

    public void setInsertIntoClauseFm(InsertIntoClauseFm insertIntoClauseFm) {
        this.insertIntoClauseFm = insertIntoClauseFm;
    }

    public ValuesClauseFragment getValuesClauseFragment() {
        return valuesClauseFragment;
    }

    public void setValuesClauseFragment(ValuesClauseFragment valuesClauseFragment) {
        this.valuesClauseFragment = valuesClauseFragment;
    }

    public StaticReturningClauseFm getStaticReturningClauseFm() {
        return staticReturningClauseFm;
    }

    public void setStaticReturningClauseFm(StaticReturningClauseFm staticReturningClauseFm) {
        this.staticReturningClauseFm = staticReturningClauseFm;
    }

    public OracleSelectStatement getOracleSelectStatement() {
        return oracleSelectStatement;
    }

    public void setOracleSelectStatement(OracleSelectStatement oracleSelectStatement) {
        this.oracleSelectStatement = oracleSelectStatement;
    }
}
