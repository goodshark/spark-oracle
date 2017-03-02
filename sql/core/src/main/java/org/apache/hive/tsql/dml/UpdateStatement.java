package org.apache.hive.tsql.dml;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.common.TreeNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by wangsm9 on 2016/12/12.
 */
public class UpdateStatement extends SqlStatement {
    private List<String> tableVariables=new ArrayList<>();
    public UpdateStatement(String name) {
        super(name);
    }


    private List<TreeNode> updateValuesNodes = new ArrayList<>();

    @Override
    public int execute() throws Exception {
        String limit = "";
        String cursorSql = "";
        for (TreeNode node : updateValuesNodes) {
            switch (node.getNodeType()) {
                case LIMIT_NUMBER:
                case LIMIT_PERCENT:
                    SqlStatement limitStatement = (SqlStatement) node;
                    limitStatement.setExecSession(getExecSession());
                    limitStatement.execute();
                    limit = limitStatement.getSql().toString();
                    break;
                case CURSOR:
                    BaseStatement baseStatement = (BaseStatement) node;
                    baseStatement.execute();
                    baseStatement.getRs();
                    // TODO 获取游标的执行结果，作为sql的一部分
                    break;

            }

        }
        String execSql = new StringBuffer().append(getSql()).append(cursorSql).append(limit).toString();
        //TODO 替换表变量
        for (String tableName: tableVariables) {
            execSql= replaceTableName(tableName,execSql);
        }
        setRs(commitStatement(execSql));
        return 0;
    }


    public void addUpdateValuesNode(TreeNode node) {
        updateValuesNodes.add(node);
    }

    public void addTableNames(Set<String> tableNames){
        tableVariables.addAll(tableNames);
    }



}
