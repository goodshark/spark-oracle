package hive.tsql.dml;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.common.TreeNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by wangsm9 on 2016/12/8.
 */
public class DeleteStatement extends SqlStatement {


    private List<String> tableVariables=new ArrayList<>();

    private List<TreeNode> delValuesNodes = new ArrayList<>();

    public DeleteStatement(String name) {
        super(name);
    }

    @Override
    public int execute() throws Exception {
        String limit = "";
        String cursorSql = "";
        for (TreeNode node : delValuesNodes) {
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
        for (String tableName:tableVariables) {
            //TODO 替换表变量
            execSql=replaceTableName(tableName,execSql);
        }
        setRs(commitStatement(execSql));
        return 1;
    }

    public void addTableNames(Set<String> tableNames){
        tableVariables.addAll(tableNames);
    }

    public void addDelValuesNode(TreeNode node) {
        delValuesNodes.add(node);
    }
}
