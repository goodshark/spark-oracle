package org.apache.hive.plsql.dml.commonFragment;

import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/4.
 * column_alias
 * : AS? (id | alias_quoted_string)
 * | AS
 */
public class ColumnAliasFragment extends SqlStatement {
    private IdFragment idFragment;
    private String alias;

    public IdFragment getIdFragment() {
        return idFragment;
    }

    public void setIdFragment(IdFragment idFragment) {
        this.idFragment = idFragment;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}
