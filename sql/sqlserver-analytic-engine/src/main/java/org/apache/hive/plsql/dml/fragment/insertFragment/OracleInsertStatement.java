package org.apache.hive.plsql.dml.fragment.insertFragment;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/11.
 * insert_statement
 * : INSERT (single_table_insert | multi_table_insert)
 * ;
 */
public class OracleInsertStatement extends SqlStatement {

    private SingleTableInsertFragment singleTableInsertFragment;
    private MultiTableInsertFragment multiTableInsertFragment;


    @Override
    public int execute() throws Exception {
        String sql = getOriginalSql();
        if (StringUtils.isNotBlank(sql)) {
            String[] musql = sql.split(";");
            for (int i = 0; i < musql.length; i++) {
                setRs(commitStatement(musql[i]));
            }
        }
        return 0;
    }

    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(" INSERT ");
        ExecSession execSession = getExecSession();
        if (null != singleTableInsertFragment) {
            sql.append(FragMentUtils.appendOriginalSql(singleTableInsertFragment, execSession));
        }
        if (null != multiTableInsertFragment) {
            String multiTableSql = FragMentUtils.appendOriginalSql(multiTableInsertFragment, execSession);
            if (StringUtils.isNotBlank(multiTableSql)) {
                String[] musql = multiTableSql.trim().split(";");
                for (int i = 0; i < musql.length; i++) {
                    sql.append(musql[i]);
                    sql.append(";");
                }
            }
        }
        return sql.toString();
    }

    public SingleTableInsertFragment getSingleTableInsertFragment() {
        return singleTableInsertFragment;
    }

    public void setSingleTableInsertFragment(SingleTableInsertFragment singleTableInsertFragment) {
        this.singleTableInsertFragment = singleTableInsertFragment;
    }

    public MultiTableInsertFragment getMultiTableInsertFragment() {
        return multiTableInsertFragment;
    }

    public void setMultiTableInsertFragment(MultiTableInsertFragment multiTableInsertFragment) {
        this.multiTableInsertFragment = multiTableInsertFragment;
    }
}
