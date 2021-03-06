package org.apache.hive.plsql.dml;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.plsql.dml.commonFragment.FragMentUtils;
import org.apache.hive.plsql.dml.commonFragment.VariableNameFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.IntoClauseFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.OrderByClauseFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.SubqueryFactoringClause;
import org.apache.hive.plsql.dml.fragment.selectFragment.SubqueryFragment;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.common.Row;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dml.SelectStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 6/8 0008.
 */
public class OracleSelectStatement extends SqlStatement {

    private static final Logger LOG = LoggerFactory.getLogger(OracleSelectStatement.class);
    private String finalSql = "";

    private SubqueryFactoringClause withQueryStatement;
    private SubqueryFragment queryBlockStatement;
    private OrderByClauseFragment orderByStatement;

    public OracleSelectStatement(String nodeName) {
        super(nodeName);
        setAddResult(true);
    }


    @Override
    public int execute() throws Exception {
        finalSql = getFinalSql();
        if (finalSql.isEmpty())
            return 0;

        List<String> commonVariableNames = new ArrayList<>();
        IntoClauseFragment intoClause = queryBlockStatement.getBasicElement().getQueryBlock().getIntoClause();
        if (null != intoClause) {
            ExecSession execSession = getExecSession();
            List<VariableNameFragment> variables = intoClause.getVariableNameFragments();
            String bulkInto = intoClause.getBulk();
            for (VariableNameFragment vnf : variables) {
                if (StringUtils.isBlank(bulkInto)) {
                    //获取变量名字
                    String vName = FragMentUtils.appendOriginalSql(vnf, execSession);
                    //TODO 判断变量类型，如果是表类型，要使用create table as
                    commonVariableNames.add(vName.trim());//普通变量

                } else {
                    //TODO 批量获取游标里面的数据，存入到一个变量里

                }
            }
            setAddResult(false);
        }
        ResultSet rs = commitStatement(finalSql);
        setRs(rs);
        if (null != intoClause)
            updateResultVar((SparkResultSet) rs, commonVariableNames);
        /*for (int i = 0; i < commonVariableNames.size(); i++) {
            String valuse = rs.getString(i);
            //TODO 设置变量的值
            commonVariableNames.get(i);
        }*/
        return 0;
    }

    public void updateResultVar(SparkResultSet resultSet, List<String> commonVariableNames) throws Exception {
        List<String> filedNames = resultSet.getFiledName();
        if (commonVariableNames.size() != filedNames.size()) {
            throw new Exception("select statements that assign values to variables cannot be used in conjunction with a data retrieval operation");
        }
       /* if (resultSet.getRsCount() > 1) {
            throw new Exception("exact fetch returns more than requested number of rows");
        }*/
        Row row = null;
        while (resultSet.next()) {
            row = resultSet.fetchRow();
        }
        for (int i = 0; i < commonVariableNames.size(); i++) {
            LOG.info("var :" + commonVariableNames.get(i) + " equals:" + row.getColumnVal(i));
            getExecSession().getVariableContainer().setVarValue(commonVariableNames.get(i), row.getColumnVal(i));
        }
    }

    @Override
    public String getSql() {
        String sql = super.getSql();
        if (sql != null)
            return sql;
        try {
            sql = getOriginalSql();
        } catch (Exception e) {
            // TODO test only
            e.printStackTrace();
        }
        super.setSql(sql);
        return sql;
    }

    @Override
    public String getOriginalSql() {
        StringBuilder sb = new StringBuilder();
        if (withQueryStatement != null) {
            withQueryStatement.setExecSession(getExecSession());
            sb.append(withQueryStatement.getOriginalSql()).append(" ");
        }
        if (queryBlockStatement != null) {
            queryBlockStatement.setExecSession(getExecSession());
            sb.append(queryBlockStatement.getOriginalSql()).append(" ");
        }
        if (orderByStatement != null) {
            orderByStatement.setExecSession(getExecSession());
            sb.append(orderByStatement.getOriginalSql());
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
