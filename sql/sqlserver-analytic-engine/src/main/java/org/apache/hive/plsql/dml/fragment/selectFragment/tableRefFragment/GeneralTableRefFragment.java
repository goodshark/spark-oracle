package org.apache.hive.plsql.dml.fragment.selectFragment.tableRefFragment;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.plsql.dml.commonFragment.TableAliasFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.DmlTableExpressionFragment;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.List;

/**
 * Created by wangsm9 on 2017/7/10.
 * <p>
 * general_table_ref
 * : (dml_table_expression_clause | ONLY '(' dml_table_expression_clause ')') table_alias?
 * ;
 */
public class GeneralTableRefFragment extends SqlStatement {
    private DmlTableExpressionFragment dmlTableExpressionFragment;
    private String only;
    private TableAliasFragment tableAliasFragment;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        if (StringUtils.isNotBlank(only)) {
            //TODO ONLY
        }
        sql.append(dmlTableExpressionFragment.getOriginalSql());
        if (null != tableAliasFragment) {
            sql.append(Common.SPACE);
            sql.append(tableAliasFragment.getOriginalSql());
        }
        return sql.toString();
    }

    public DmlTableExpressionFragment getDmlTableExpressionFragment() {
        return dmlTableExpressionFragment;
    }

    public void setDmlTableExpressionFragment(DmlTableExpressionFragment dmlTableExpressionFragment) {
        this.dmlTableExpressionFragment = dmlTableExpressionFragment;
    }

    public String getOnly() {
        return only;
    }

    public void setOnly(String only) {
        this.only = only;
    }

    public TableAliasFragment getTableAliasFragment() {
        return tableAliasFragment;
    }

    public void setTableAliasFragment(TableAliasFragment tableAliasFragment) {
        this.tableAliasFragment = tableAliasFragment;
    }
}
