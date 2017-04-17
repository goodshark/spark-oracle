package org.apache.hive.tsql.dml;


import org.apache.commons.lang.StringUtils;
import org.apache.hive.tsql.common.*;
import org.apache.hive.tsql.dml.select.SelectIntoBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SelectStatement extends SqlStatement {

    private static final Logger LOG = LoggerFactory.getLogger(SelectStatement.class);

    public SelectStatement(String selectStatementName) {
        super(selectStatementName);
        setAddResult(true);
    }

    /**
     * 保存sql中的变量名字,对应于g4文件中的localId
     * 如 WHERE OrganizationLevel > @cond AND OrganizationLevel < @cond sql中的@cond
     */
    private Set<String> localIdVariableName = new HashSet<String>();


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
            if (getRs().getRow() <= 0) {
                throw new Exception(" it has not resultSet in select statement");
            } else {
                updateResultVar((SparkResultSet) getRs());
            }
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
        execSQL = getSql();
        execSQL = replaceVariable(execSQL, localIdVariableName);
        replaceTableNames();
    }


    private void selectIntoExec() throws Exception {
        if (null != selectIntoBean && !StringUtils.isBlank(selectIntoBean.getTableNanme())) {
            String tableName = selectIntoBean.getTableNanme();
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

    public void addVariables(Set<String> variables) {
        localIdVariableName.addAll(variables);
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
