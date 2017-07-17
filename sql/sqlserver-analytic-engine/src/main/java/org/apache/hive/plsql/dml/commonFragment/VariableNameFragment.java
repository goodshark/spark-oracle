package org.apache.hive.plsql.dml.commonFragment;

import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wangsm9 on 2017/7/11.
 * variable_name
 * : (INTRODUCER char_set_name)? id_expression ('.' id_expression)?
 * | bind_variable
 */
public class VariableNameFragment extends SqlStatement {

    private String introducer;
    private CharSetNameFragment charSetNameFragment;
    private List<ExpressionStatement> idExpressins = new ArrayList<>();
    private BindVariableNameFm bindVariableNameFm;

    @Override
    public String getOriginalSql() {
        StringBuffer sql = new StringBuffer();
        if (null != charSetNameFragment) {
            sql.append("INTRODUCER ");
            sql.append(charSetNameFragment.getOriginalSql());
        }
        if (!idExpressins.isEmpty()) {
            if (idExpressins.size() == 1) {
                sql.append(idExpressins.get(0));
            } else if (idExpressins.size() == 2) {
                sql.append(idExpressins.get(0));
                sql.append(".");
                sql.append(idExpressins.get((1)));
            }
        }
        if (null != bindVariableNameFm) {
            sql.append(bindVariableNameFm.getOriginalSql());
        }
        return sql.toString();
    }

    public String getIntroducer() {
        return introducer;
    }

    public void setIntroducer(String introducer) {
        this.introducer = introducer;
    }

    public CharSetNameFragment getCharSetNameFragment() {
        return charSetNameFragment;
    }

    public void setCharSetNameFragment(CharSetNameFragment charSetNameFragment) {
        this.charSetNameFragment = charSetNameFragment;
    }

    public BindVariableNameFm getBindVariableNameFm() {
        return bindVariableNameFm;
    }

    public void setBindVariableNameFm(BindVariableNameFm bindVariableNameFm) {
        this.bindVariableNameFm = bindVariableNameFm;
    }

    public void addIdExpression(ExpressionStatement s) {
        idExpressins.add(s);
    }

}
