package org.apache.hive.plsql.dml.fragment.selectFragment;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.plsql.dml.commonFragment.ColumnAliasFragment;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

/**
 * Created by dengrb1 on 6/9 0009.
 * <p>
 * selected_element
 * : select_list_elements column_alias?
 * ;
 */
public class SelectElementFragment extends SqlStatement {
    private SelectListElementsFragment col;
    private ColumnAliasFragment colAlias;

    public void setCol(SelectListElementsFragment c) {
        col = c;
    }

    public void setColAlias(ColumnAliasFragment a) {
        colAlias = a;
    }

    @Override
    public String getSql() {
        String sql = super.getSql();
        if (sql != null)
            return sql;
        sql = col + " " + colAlias;
        super.setSql(sql);
        return sql;
    }

    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(FragMentUtils.appendOriginalSql(col,getExecSession()));
        if (null != colAlias) {
            sql.append(FragMentUtils.appendOriginalSql(colAlias,getExecSession()));
        }
        return sql.toString();
    }

}
