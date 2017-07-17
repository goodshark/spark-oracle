package org.apache.hive.plsql.dml.commonFragment;

import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

/**
 * Created by wangsm9 on 2017/7/3.
 * id
 * : (INTRODUCER char_set_name)? id_expression
 * ;
 */
public class IdFragment extends SqlStatement {

    private CharSetNameFragment charSetName;

    private ExpressionStatement idExpression;


    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        if (null != charSetName) {
            sql.append("INTRODUCER  ");
            sql.append(charSetName.getOriginalSql());
        }
        sql.append(idExpression.getOriginalSql());
        return  sql.toString();
    }


    public SqlStatement getCharSetName() {
        return charSetName;
    }

    public void setCharSetName(CharSetNameFragment charSet) {
        charSetName = charSet;
    }

    public ExpressionStatement getIdExpression() {
        return idExpression;
    }

    public void setIdExpression(ExpressionStatement idExpression) {
        this.idExpression = idExpression;
    }
}
