package org.apache.hive.plsql.dml;

import org.apache.hive.plsql.dml.fragment.selectFragment.OrderByClauseFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.SubqueryFactoringClause;
import org.apache.hive.plsql.dml.fragment.selectFragment.SubqueryFragment;
import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by dengrb1 on 6/8 0008.
 */
public class OracleSelectStatement extends SqlStatement {
    private String finalSql = "";

    private SubqueryFactoringClause withQueryStatement;
    private SubqueryFragment queryBlockStatement;
    private OrderByClauseFragment orderByStatement;

    public OracleSelectStatement(String nodeName) {
        super(nodeName);
        setAddResult(true);
    }

    private void genFinalSql() throws Exception {
        if (withQueryStatement != null) {
            finalSql += withQueryStatement.getFinalSql();
        }
        if (queryBlockStatement == null)
            throw new Exception("missing query block statement in gen final sql");
        finalSql += queryBlockStatement.getFinalSql();

        if (orderByStatement != null) {
            finalSql += orderByStatement.getFinalSql();
        }
    }

    private String genOriginalSql() throws Exception {
        StringBuilder sb = new StringBuilder();
        if (withQueryStatement != null) {
            sb.append(withQueryStatement.getOriginalSql());
        }
        if (queryBlockStatement == null)
            throw new Exception("missing query block statement in gen original sql");
        sb.append(queryBlockStatement.getOriginalSql());
        if (orderByStatement != null) {
            sb.append(orderByStatement.getOriginalSql());
        }
        return sb.toString();
    }

    @Override
    public int execute() throws Exception {
        // TODO check all variable in original sql
//        genFinalSql();
        finalSql = getFinalSql();
        if (finalSql.isEmpty())
            return 0;
        setRs(commitStatement(finalSql));
        // TODO check into ?
        return 0;
    }

    @Override
    public String getSql() {
        String sql = super.getSql();
        if (sql != null)
            return sql;
        try {
            sql = genOriginalSql();
        } catch (Exception e) {
            // TODO test only
            e.printStackTrace();
        }
        super.setSql(sql);
        return sql;
    }

    @Override
    public String getOriginalSql() {
        return "";
    }

    @Override
    public String getFinalSql() throws Exception {
        StringBuilder sb = new StringBuilder();
        if (withQueryStatement != null) {
            withQueryStatement.setExecSession(getExecSession());
            sb.append(withQueryStatement.getFinalSql()).append(" ");
        }
        if (queryBlockStatement != null) {
            queryBlockStatement.setExecSession(getExecSession());
            sb.append(queryBlockStatement.getFinalSql()).append(" ");
        }
        if (orderByStatement != null) {
            orderByStatement.setExecSession(getExecSession());
            sb.append(orderByStatement.getFinalSql());
        }
        return sb.toString();
    }

    public SubqueryFactoringClause getWithQueryStatement() {
        return withQueryStatement;
    }

    public void setWithQueryStatement(SubqueryFactoringClause withQueryStatement) {
        this.withQueryStatement = withQueryStatement;
    }

    public SubqueryFragment getQueryBlockStatement() {
        return queryBlockStatement;
    }

    public void setQueryBlockStatement(SubqueryFragment queryBlockStatement) {
        this.queryBlockStatement = queryBlockStatement;
    }

    public OrderByClauseFragment getOrderByStatement() {
        return orderByStatement;
    }

    public void setOrderByStatement(OrderByClauseFragment orderByStatement) {
        this.orderByStatement = orderByStatement;
    }
}
