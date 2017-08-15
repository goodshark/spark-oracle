package org.apache.hive.tsql.common;


import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.udf.ObjectIdCalculator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by wangsm9 on 2016/11/24.
 */
public class SqlStatement extends BaseStatement implements Serializable {

    private static final long serialVersionUID = -1531515791432293303L;
    private static final Logger LOG = LoggerFactory.getLogger(SqlStatement.class);

    public SqlStatement() {
    }

    public SqlStatement(String name) {
        super(name);
    }

    @Override
    public int execute() throws Exception {
        setRs(commitStatement(getSql()));
        return 1;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }


    /**
     * 保存sql中的变量名字
     * 如 insert into test_person values(@a,20+9,55.5,'1945-3-5','');
     */
    public Set<String> localIdVariableName = new HashSet<String>();

    public void addVariables(Set<String> variables) {
        localIdVariableName.addAll(variables);
    }

    /**
     * 替换sql中的表名
     *
     * @param tableName
     * @param sql
     */
    public String replaceTableName(String tableName, String sql) throws Exception {
        String realTableName =getExecSession().getRealTableName(tableName);
        return sql.replaceAll(tableName, realTableName);
    }

    public String replaceVariable(String sql, Set<String> variable) throws Exception{
        if (!variable.isEmpty()) {
            for (String s : variable) {
                Var v = s.startsWith("@@") ? findSystemVar(s) : findVar(s);
                if(v==null){
                    LOG.error("variable:" + s + " not defined");
                }else{
                    sql = sql.replaceAll(s, null == v.getVarValue() ? "null": v.getExecString());
                }
            }
        }
        return sql;
    }

    public String getOriginalSql()  {
        return super.getSql();
    }

    public String getFinalSql() throws Exception {
        return getOriginalSql();
    }
    public String getSchemaTableName(String tableName){
        if(tableName.contains(".")){
            String[] tbs = tableName.split("\\.");
            return tbs[0]+"."+"dbo."+tbs[1];
        }
        return  tableName;
    }

    public void checkTableIsExist(List<String> tableNames,String type) throws Exception{
        ObjectIdCalculator objectIdCalculator = new ObjectIdCalculator();
        objectIdCalculator.setExecSession(getExecSession());
        for (String tableName : tableNames) {
            String t = getSchemaTableName(tableName);
            boolean b1=objectIdCalculator.databaseFind(t, type);
            if(!b1){
                throw new Exception("Table or view :"+tableName +" is not exist.");
            }
        }
    }

}
