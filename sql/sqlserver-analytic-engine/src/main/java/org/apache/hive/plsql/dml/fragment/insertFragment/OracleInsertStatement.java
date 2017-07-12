package org.apache.hive.plsql.dml.fragment.insertFragment;

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
