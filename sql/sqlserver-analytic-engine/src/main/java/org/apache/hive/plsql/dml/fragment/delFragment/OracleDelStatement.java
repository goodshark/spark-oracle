package org.apache.hive.plsql.dml.fragment.delFragment;

import org.apache.hive.plsql.dml.fragment.selectFragment.WhereClauseFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.tableRefFragment.GeneralTableRefFragment;
import org.apache.hive.plsql.dml.fragment.updateFragment.StaticReturningClauseFm;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/11.
 * <p>
 * delete_statement
 * : DELETE FROM? general_table_ref where_clause? static_returning_clause? error_logging_clause?
 * ;
 */
public class OracleDelStatement extends SqlStatement {


    private GeneralTableRefFragment generalTableRefFragment;
    private WhereClauseFragment whereClauseFragment;
    private StaticReturningClauseFm staticReturningClauseFm;

    public GeneralTableRefFragment getGeneralTableRefFragment() {
        return generalTableRefFragment;
    }

    public void setGeneralTableRefFragment(GeneralTableRefFragment generalTableRefFragment) {
        this.generalTableRefFragment = generalTableRefFragment;
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
