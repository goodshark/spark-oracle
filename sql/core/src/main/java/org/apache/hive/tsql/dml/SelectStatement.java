package hive.tsql.dml;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.Row;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.util.StrUtils;
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
                throw new Exception("向变量赋值的 SELECT 语句没有结果集");
            } else {
                updateResultVar((SparkResultSet) getRs());
            }
        }


        try {
            if (getRs().getRow() > 0) {

            }
        } catch (Exception e) {
            e.printStackTrace();
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
            throw new Exception("向变量赋值的 SELECT 语句不能与数据检索操作结合使用");
        }
        Row row = null;
        while (resultSet.next()) {
            row = resultSet.fetchRow();
        }
        for (int i = 0; i < resultSetVariable.size(); i++) {
            LOG.info("对变量:" + resultSetVariable.get(i) + " 赋值:" + row.getColumnVal(i));
            getExecSession().getVariableContainer().setVarValue(resultSetVariable.get(i), row.getColumnVal(i));
        }
    }

    public void init() throws Exception {
        replaceVariable();
        replaceTableNames();
    }

    private void replaceTableNames() {
        if (!tableNames.isEmpty()) {
            for (String tableName : tableNames) {
                execSQL = replaceTableName(tableName, execSQL);
            }
        }
    }

    private void replaceVariable() throws Exception {
        Set<String> localIdVariable = getLocalIdVariable();
        execSQL = getSql();
        if (!localIdVariable.isEmpty()) {
            for (String s : localIdVariable) {
                Var v = s.startsWith("@@") ? findSystemVar(s) : findVar(s);
                if(v==null){
                    throw new Exception("变量:"+s+" 没有定义");
                }
//                String value = v.getVarValue().toString();
                execSQL = execSQL.replaceAll(s, null == v.getVarValue() ? StrUtils.addQuot(""): StrUtils.addQuot(v.getVarValue().toString()));
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


}
