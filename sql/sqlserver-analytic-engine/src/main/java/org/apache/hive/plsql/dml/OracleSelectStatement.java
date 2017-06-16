package org.apache.hive.plsql.dml;

import org.apache.hive.tsql.common.SqlStatement;

/**
 * Created by dengrb1 on 6/8 0008.
 */
public class OracleSelectStatement extends SqlStatement {
    private String finalSql = "";

    private SqlStatement withQueryStatement = null;
    private SqlStatement queryBlockStatement = null;
    private SqlStatement forClauseStatement = null;
    private SqlStatement orderByStatement = null;

    public OracleSelectStatement(String nodeName) {
        super(nodeName);
        setAddResult(true);
    }

    public void setWithQueryStatement(SqlStatement withQuery) {
        withQueryStatement = withQuery;
    }

    public void setQueryBlockStatement(SqlStatement queryBlock) {
        queryBlockStatement = queryBlock;
    }

    public void setForClauseStatement(SqlStatement forClause) {
        forClauseStatement = forClause;
    }

    public void setOrderByStatement(SqlStatement orderBy) {
        orderByStatement = orderBy;
    }

    private void genFinalSql() throws Exception {
        if (withQueryStatement != null) {
            finalSql += withQueryStatement.getFinalSql();
        }
        if (queryBlockStatement == null)
            throw new Exception("missing query block statement in gen final sql");
        finalSql += queryBlockStatement.getFinalSql();
        if (forClauseStatement != null) {
            finalSql += forClauseStatement.getFinalSql();
        }
        if (orderByStatement != null) {
            finalSql += orderByStatement.getFinalSql();
        }
    }

    private String genOriginalSql() throws Exception {
        StringBuilder sb = new StringBuilder() ;
        if (withQueryStatement != null) {
            sb.append(withQueryStatement.getOriginalSql());
        }
        if (queryBlockStatement == null)
            throw new Exception("missing query block statement in gen original sql");
        sb.append(queryBlockStatement.getOriginalSql());
        if (forClauseStatement != null) {
            sb.append(forClauseStatement.getOriginalSql());
        }
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
        if (forClauseStatement != null) {
            forClauseStatement.setExecSession(getExecSession());
            sb.append(forClauseStatement.getFinalSql()).append(" ");
        }
        if (orderByStatement != null) {
            orderByStatement.setExecSession(getExecSession());
            sb.append(orderByStatement.getFinalSql());
        }
        return sb.toString();
    }
}
