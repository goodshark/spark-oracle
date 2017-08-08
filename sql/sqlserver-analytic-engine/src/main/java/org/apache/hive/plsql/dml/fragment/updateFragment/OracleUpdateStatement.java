package org.apache.hive.plsql.dml.fragment.updateFragment;

import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.fragment.selectFragment.WhereClauseFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.tableRefFragment.GeneralTableRefFragment;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/10.
 * <p>
 * update_statement
 * : UPDATE general_table_ref update_set_clause where_clause? static_returning_clause? error_logging_clause?
 * ;
 */
public class OracleUpdateStatement extends SqlStatement {

    private GeneralTableRefFragment generalTableRefFragment;
    private UpdateSetClauseFm updateSetClauseFm;
    private WhereClauseFragment whereClauseFragment;
    private StaticReturningClauseFm staticReturningClauseFm;


    @Override
    public int execute() throws Exception {
        String sql = getOriginalSql();
        setRs(commitStatement(sql));
        return 0;
    }

    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append("UPDATE ");
        ExecSession execSession = getExecSession();
        sql.append(FragMentUtils.appendOriginalSql(generalTableRefFragment, execSession));
        sql.append(FragMentUtils.appendOriginalSql(updateSetClauseFm, execSession));
        if (null != whereClauseFragment) {
            sql.append(FragMentUtils.appendOriginalSql(whereClauseFragment, execSession));
        }
        if (null != staticReturningClauseFm) {
            //TODO
        }
        // TODO error_logging_clause
        return sql.toString();
    }

    public GeneralTableRefFragment getGeneralTableRefFragment() {
        return generalTableRefFragment;
    }

    public void setGeneralTableRefFragment(GeneralTableRefFragment generalTableRefFragment) {
        this.generalTableRefFragment = generalTableRefFragment;
    }

    public UpdateSetClauseFm getUpdateSetClauseFm() {
        return updateSetClauseFm;
    }

    public void setUpdateSetClauseFm(UpdateSetClauseFm updateSetClauseFm) {
        this.updateSetClauseFm = updateSetClauseFm;
    }

    public WhereClauseFragment getWhereClauseFragment() {
        return whereClauseFragment;
    }

    public void setWhereClauseFragment(WhereClauseFragment whereClauseFragment) {
        this.whereClauseFragment = whereClauseFragment;
    }

    public StaticReturningClauseFm getStaticReturningClauseFm() {
        return staticReturningClauseFm;
    }

    public void setStaticReturningClauseFm(StaticReturningClauseFm staticReturningClauseFm) {
        this.staticReturningClauseFm = staticReturningClauseFm;
    }
}
