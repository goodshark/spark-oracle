package org.apache.hive.plsql.dml.fragment.insertFragment;

import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/11.
 * multi_table_element
 * : insert_into_clause values_clause? error_logging_clause?
 * ;
 */
public class MultiTableElementFm extends SqlStatement {
    private InsertIntoClauseFm insertIntoClauseFm;
    private ValuesClauseFragment valuesClauseFragment;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        ExecSession execSession = getExecSession();
        if (null != insertIntoClauseFm) {
            sql.append(FragMentUtils.appendOriginalSql(insertIntoClauseFm, execSession));
        }
        if (null != insertIntoClauseFm) {
            sql.append(FragMentUtils.appendOriginalSql(valuesClauseFragment, execSession));
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
}
