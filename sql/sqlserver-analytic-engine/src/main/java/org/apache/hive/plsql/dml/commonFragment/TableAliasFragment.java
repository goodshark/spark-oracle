package org.apache.hive.plsql.dml.commonFragment;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/10.
 * <p>
 * table_alias
 * : (id | alias_quoted_string)
 * ;
 */
public class TableAliasFragment extends SqlStatement {

    private IdFragment idFragment;
    private String alias;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        if (null != idFragment) {
            sql.append(idFragment.getOriginalSql());
        }
        if (StringUtils.isNotBlank(alias)) {
            sql.append(alias);
        }
        return sql.toString();
    }

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
