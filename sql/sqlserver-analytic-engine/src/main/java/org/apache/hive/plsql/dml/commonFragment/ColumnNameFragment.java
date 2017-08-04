package org.apache.hive.plsql.dml.commonFragment;

import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

import java.util.ArrayList;
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

    private List<ExpressionStatement> idExpressions = new ArrayList<>();

    public void addExpress(ExpressionStatement expressionStatement) {
        idExpressions.add(expressionStatement);
    }

    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        sql.append(id.getOriginalSql());
        for (ExpressionStatement id : idExpressions) {
            sql.append(".");
            sql.append(FragMentUtils.appendOriginalSql(id, getExecSession()));
        }
        return sql.toString();
    }

    public IdFragment getId() {
        return id;
    }

    public void setId(IdFragment id) {
        this.id = id;
    }


}
