package org.apache.hive.plsql.dml.fragment;

import org.apache.hive.tsql.common.SqlStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 6/14 0014.
 */
public class CommonFragment extends SqlStatement {
    // "xxxFragment", "SqlStatement", "ExpressionStatement", ...
    private List<SqlStatement> fragments = new ArrayList<>();

    public void addFragment(SqlStatement stmt) {
        fragments.add(stmt);
    }

    public String getOriginalSql() {
        StringBuilder sb = new StringBuilder();
        for (SqlStatement stmt: fragments) {
            sb.append(stmt.getOriginalSql()).append(" ");
        }
        return sb.toString();
    }

    public String getFinalSql() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (SqlStatement stmt: fragments) {
            stmt.setExecSession(getExecSession());
            sb.append(stmt.getFinalSql()).append(" ");
        }
        return sb.toString();
    }
}
