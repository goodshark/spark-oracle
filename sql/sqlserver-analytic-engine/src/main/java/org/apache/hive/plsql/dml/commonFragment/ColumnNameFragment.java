package org.apache.hive.plsql.dml.commonFragment;

import org.apache.hive.tsql.SqlserverEngine;
import org.apache.hive.tsql.common.SqlStatement;

import java.util.List;

/**
 * Created by wangsm9 on 2017/7/3.
 */
public class ColumnNameFragment extends SqlStatement {

    private IdFragment id;

    private List<String> idExpressions;

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
