package org.apache.hive.tsql.dml;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.*;
import org.apache.hive.tsql.util.StrUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by wangsm9 on 2016/12/12.
 */
public class InsertStatement extends SqlStatement {
    private static final Logger LOG = LoggerFactory.getLogger(InsertStatement.class);

    private List<String> tableVariables=new ArrayList<>();

    private List<TreeNode> insertValuesNodes = new ArrayList<>();

    public InsertStatement(String name) {
        super(name);
    }

    @Override
    public int execute() throws Exception {
        if (null == insertValuesNodes || insertValuesNodes.size() < 1) {
            return -1;
        }
        String resultSql = "";
        String limit = "";
        if (insertValuesNodes.size() == 2) {
            //第一个孩子为limt，第二个孩子为insertValue
            limit = getSqlFromChidrenNode(insertValuesNodes.get(0));
            resultSql = getSqlFromChidrenNode(insertValuesNodes.get(1));
        } else {
            resultSql = getSqlFromChidrenNode(insertValuesNodes.get(0));
        }
        String execSql = new StringBuffer().append(getSql()).
                append(Common.SPACE).append(resultSql)
                .append(Common.SPACE).append(limit).toString();

        //TODO 替换sql中的表变量
        if(!tableVariables.isEmpty()){
            for (String tableName:tableVariables) {
                execSql = replaceTableName(tableName, execSql);
            }
        }
        setRs(commitStatement(execSql));
        return 0;
    }


    private String getSqlFromChidrenNode(TreeNode treeNode) throws Exception {
        Type nodeType = treeNode.getNodeType();
        String resultSql = "";
        switch (nodeType) {
            case TABLE_VALUE:
            case DERIVED_TABLE:
                SqlStatement sqlStatement = (SqlStatement) treeNode;
                resultSql = replaceVariable(sqlStatement.getSql().toString());
                break;
            case TABEL_DEFAULT_VALUES:
                treeNode.execute();
                treeNode.setExecSession(getExecSession());
                resultSql = treeNode.getRs().getObject(0).toString();
                break;
            case EXECUTE_STATEMENT:
                //执行的结果作为sql的一部分
                //values(1,'b1-1',11),(2,'b1-2',12),(3,'b1-3',13)

                treeNode.setExecSession(getExecSession());
                treeNode.execute();
                SparkResultSet sparkResultSet = (SparkResultSet) treeNode.getRs();
                if(null==sparkResultSet){
                    throw new Exception( "it has not resultSet to insert ");
                }
                StringBuffer sql = new StringBuffer();
                sql.append(" values");
                int columnSize = sparkResultSet.getColumnSize();
                while (sparkResultSet.next()) {
                    sql.append("(");
                    Row row = sparkResultSet.fetchRow();
                    for (int i = 0; i < columnSize; i++) {
                        if (i != 0 && i != columnSize) {
                            sql.append(",");
                        }
                        sql.append(StrUtils.addQuot(row.getColumnVal(i).toString()));
                    }
                    sql.append("),");
                }
                resultSql = sql.substring(0, sql.length() - 1);
                break;
            case LIMIT_NUMBER:
            case LIMIT_PERCENT:
                SqlStatement limitStatement = (SqlStatement) treeNode;
                limitStatement.execute();
                treeNode.setExecSession(getExecSession());
                resultSql = limitStatement.getSql();
                break;
        }
        return resultSql;
    }




    /**
     * 保存sql中的变量名字
     * 如 insert into test_person values(@a,20+9,55.5,'1945-3-5','');
     */
    private Set<String> localIdVariableName = new HashSet<String>();

    public void addVariables(Set<String> variables) {
        localIdVariableName.addAll(variables);
    }

    private String replaceVariable(String sql) throws Exception {
        if (!localIdVariableName.isEmpty()) {
            for (String s : localIdVariableName) {
                Var v = s.startsWith("@@") ? findSystemVar(s) : findVar(s);
                if (v == null) {
                    throw new Exception("variable:" + s + " not defined");
                }
//                String value = v.getVarValue().toString();
                sql = sql.replaceAll(s, null == v.getVarValue() ? "" : "'" + v.getVarValue().toString() + "'");
            }
        }
        return sql;
    }

    public void addInsertValuesNode(TreeNode node) {
        insertValuesNodes.add(node);
    }

    public void addTableNames(Set<String> tableNames){
        tableVariables.addAll(tableNames);
    }

}
