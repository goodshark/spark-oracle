package org.apache.hive.plsql.dml.fragment;

import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by dengrb1 on 6/9 0009.
 */
public class BasicElementFragment extends SqlStatement {
    private SqlStatement queryBlock = null;
    private SqlStatement subQuery = null;

    @Override
    public String getSql() {
        return "";
    }
}
