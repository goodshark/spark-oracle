package org.apache.hive.plsql.dml.commonFragment;

import org.apache.hive.tsql.common.SqlStatement;

import java.util.List;

/**
 * Created by wangsm9 on 2017/7/3.
 * <p>
 * column_name
 * : id ('.' id_expression)*
 * ;
 */
public class ColumnNameFragment extends SqlStatement {

    private IdFragment id;

    private List<String> idExpressions;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(id.getOriginalSql());
        for (String id : idExpressions) {
            sql.append(".");
            sql.append(id);
        }
        return sql.toString();
    }

    public SqlStatement getId() {
        return id;
    }

    public void setId(IdFragment id) {
        this.id = id;
    }

    public List<String> getIdExpressions() {
        return idExpressions;
    }

    public void setIdExpressions(List<String> idExpressions) {
        this.idExpressions = idExpressions;
    }
}
