package org.apache.hive.plsql.dml.commonFragment;

import org.apache.hive.tsql.common.SqlStatement;

import java.util.List;

/**
 * Created by wangsm9 on 2017/7/3.
 */
public class CharSetNameFragment extends SqlStatement {


    private List<String> idExpressions;

    public List<String> getIdExpressions() {
        return idExpressions;
    }

    public void setIdExpressions(List<String> idExpressions) {
        this.idExpressions = idExpressions;
    }
}
