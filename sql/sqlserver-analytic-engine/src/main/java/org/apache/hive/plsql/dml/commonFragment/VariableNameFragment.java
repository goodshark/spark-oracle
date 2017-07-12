package org.apache.hive.plsql.dml.commonFragment;

import org.apache.hive.tsql.common.SqlStatement;

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
    private List<String> idExpressins;

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

    private BindVariableNameFm bindVariableNameFm;

    public void addIdExpression(String s) {
        idExpressins.add(s);
    }

}
