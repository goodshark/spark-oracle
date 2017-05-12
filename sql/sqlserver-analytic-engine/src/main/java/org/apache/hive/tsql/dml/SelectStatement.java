package org.apache.hive.tsql.dml;


import org.apache.commons.lang.StringUtils;
import org.apache.hive.tsql.common.*;
import org.apache.hive.tsql.dml.select.SelectIntoBean;

import org.apache.hive.tsql.util.StrUtils;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Predef;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class SelectStatement extends SqlStatement {

    private static final Logger LOG = LoggerFactory.getLogger(SelectStatement.class);

    public SelectStatement(String selectStatementName) {
        super(selectStatementName);
        setAddResult(true);
    }

    /**
     * 将执行sql的结果 赋于一个变量
     * 如select  @aa=string2  from boolean_ana where string1 ='code'
     */
    private List<String> resultSetVariable = new ArrayList<>();


    /**
     * 将所有的tableName 放在一个变量里
     * 如select * from ##table，@tableName1
     */
    private List<String> tableNames = new ArrayList<>();

    private String execSQL = "";


    private SelectIntoBean selectIntoBean;

    @Override
    public int execute() throws Exception {
        String limit = "";
        List<TreeNode> list = getChildrenNodes();
        for (TreeNode node : list) {
            switch (node.getNodeType()) {
                case LIMIT_NUMBER:
                case LIMIT_PERCENT:
                    SqlStatement limitStatement = (SqlStatement) node;
                    limitStatement.execute();
                    limit = limitStatement.getSql().toString();
                    break;
            }
        }
        init();
        execSQL = execSQL + limit;
        setRs(commitStatement(execSQL));
        //TODO 如果select中的含有结果,将改变变量的值
        if (resultSetVariable.size() > 0) {
            updateResultVar((SparkResultSet) getRs());
        }
        return 1;
    }

    /**
     * SELECT @v_name=name+'11',age from dbo.a1
     * [SQL Server]向变量赋值的 SELECT 语句不能与数据检索操作结合使用。
     * <p>
     * 正确写法为   SELECT @v_name=name+'11',@v_name=age from dbo.a1
     * <p>
     * PRINT @v_name
     *
     * @param resultSet
     * @throws Exception
     */

    public void updateResultVar(SparkResultSet resultSet) throws Exception {
        List<String> filedNames = resultSet.getFiledName();
        if (resultSetVariable.size() != filedNames.size()) {
            throw new Exception("select statements that assign values to variables cannot be used in conjunction with a data retrieval operation");
        }
        if (resultSet.getRow() <= 0) {
            //结果集中没有结果，将对变量赋值为null
            for (int i = 0; i < resultSetVariable.size(); i++) {
                getExecSession().getVariableContainer().setVarValue(resultSetVariable.get(i), null);
            }
            return;
        }

        Row row = null;
        while (resultSet.next()) {
            row = resultSet.fetchRow();
        }
        for (int i = 0; i < resultSetVariable.size(); i++) {
            LOG.info("var :" + resultSetVariable.get(i) + " equals:" + row.getColumnVal(i));
            getExecSession().getVariableContainer().setVarValue(resultSetVariable.get(i), row.getColumnVal(i));
        }
    }

    public void init() throws Exception {
         selectIntoExec();
        LOG.info("11===>"+execSQL);
        execSQL = getSql();
        LOG.info("22===>"+execSQL);
        execSQL = replaceVariable(execSQL, localIdVariableName);
        LOG.info("33===>"+execSQL);
        replaceTableNames();
        LOG.info("44===>"+execSQL);
        replaceCrudClusterByColumn();
        LOG.info("55===>"+execSQL);
    }


    private void selectIntoExec() throws Exception {
        if (null != selectIntoBean && !StringUtils.isBlank(selectIntoBean.getIntoTableName())) {
            String tableName = selectIntoBean.getIntoTableName();
            TmpTableNameUtils tmpTableNameUtils = new TmpTableNameUtils();
            String sql = "DROP TABLE IF EXISTS " + tableName;
            //如果是局部临时表，需要删除
            if (tmpTableNameUtils.checkIsTmpTable(tableName)) {
                String realTableName = getExecSession().getRealTableName(tableName);
                LOG.info("realTableName:" + realTableName + ",orcTableName:" + tableName);
                if (!StringUtils.equals(tableName, realTableName)) {
                    sql = replaceTableName(tableName, sql);
                    commitStatement(sql);
                }

            }
        }
    }

    private void replaceTableNames() throws Exception {
        if (!tableNames.isEmpty()) {
            for (String tableName : tableNames) {
                execSQL = replaceTableName(tableName, execSQL);
            }
        }
    }


    private void replaceCrudClusterByColumn() throws Exception {

        boolean a = (null != selectIntoBean);
        LOG.info("========>" + a);
        String clusterByColumn = "";
        if (null != selectIntoBean) {
            clusterByColumn = selectIntoBean.getClusterByColumnName();
            if(StringUtils.isBlank(clusterByColumn)){
                LOG.info("current sql is " +  execSQL);
                LOG.info("create crud table : clusterbyColumnName:" +  selectIntoBean.getClusterByColumnName());
                LOG.info("create crud table : fromTb:" +  selectIntoBean.getSourceTableName());
                String fromTableName = selectIntoBean.getSourceTableName();
                if (fromTableName.contains(".")) {
                    fromTableName = fromTableName.split("\\.")[1];
                }
                try {
                    TableIdentifier tableIdeentifier = new TableIdentifier(fromTableName);
                    CatalogTable tableMetadata = getExecSession().getSparkSession().sessionState().catalog().getTableMetadata(tableIdeentifier);
                    if (null != tableMetadata) {
                        clusterByColumn = tableMetadata.schema().fieldNames()[0];
                    }
                } catch (Exception e) {
                    LOG.error(" create crud table:" + selectIntoBean.getIntoTableName() + " cluster by cloumn name error.", e);
                }

                if (StringUtils.isBlank(clusterByColumn)) {
                    throw new Exception(" create  crud table: " + selectIntoBean.getIntoTableName() + " failed.");
                }
            }else{
                execSQL = String.format(execSQL, StrUtils.addBackQuote(clusterByColumn));
            }
        }
    }


    public void addResultSetVariables(List<String> variables) {
        resultSetVariable.addAll(variables);
    }

    public void addTableNames(Set<String> tableNames) {
        this.tableNames.addAll(tableNames);
    }

    public Set<String> getLocalIdVariable() {
        return localIdVariableName;
    }

    public List<String> getResultSetVariable() {
        return resultSetVariable;
    }

    public SelectIntoBean getSelectIntoBean() {
        return selectIntoBean;
    }

    public void setSelectIntoBean(SelectIntoBean selectIntoBean) {
        this.selectIntoBean = selectIntoBean;
    }
}
