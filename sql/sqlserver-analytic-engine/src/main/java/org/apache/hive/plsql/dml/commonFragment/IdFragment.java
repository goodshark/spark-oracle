package org.apache.hive.plsql.dml.commonFragment;

import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by wangsm9 on 2017/7/3.
 */
public class IdFragment extends SqlStatement {

    private CharSetNameFragment CharSetName;

    private String idExpression;

    public SqlStatement getCharSetName() {
        return CharSetName;
    }

    public void setCharSetName(CharSetNameFragment charSetName) {
        CharSetName = charSetName;
    }

    public String getIdExpression() {
        return idExpression;
    }

    public void setIdExpression(String idExpression) {
        this.idExpression = idExpression;
    }
}
