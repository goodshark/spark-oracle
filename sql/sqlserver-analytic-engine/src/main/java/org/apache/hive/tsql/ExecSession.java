package org.apache.hive.tsql;

import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;
import org.apache.commons.lang.StringUtils;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.arg.VariableContainer;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.RootNode;
import org.apache.hive.tsql.common.TmpTableNameUtils;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhongdg1 on 2016/11/29.
 */
public class ExecSession {

    private static final Logger LOG = LoggerFactory.getLogger(ExecSession.class);
    private List<LogicalPlan> logicalPlans = new ArrayList<>();
    private TreeNode rootNode;
    private VariableContainer variableContainer;
    private SparkSession sparkSession;
    private List<ResultSet> resultSets;
    private Stack<BaseStatement> jumpCmds = new Stack<>();
    //    private List<Exception> exceptions;
    private AbstractParseTreeVisitor visitor;
    private boolean isReset = true;
    private String errorStr = "";
    private String database = "default";

    private boolean packageScope = false;
    private String packageName = "";

    public void setPackageScope(String name) {
        packageScope = true;
        packageName = name;
    }

    public void clearePackageScope() {
        packageScope = false;
        packageName = "";
    }

    public boolean isPackageScope() {
        return packageScope;
    }

    public String getPackageName() {
        return packageName;
    }


    // mark break/continue/goto/return/raise/throw cmd
    public enum Scope {
        BEGIN, IF, WHILE, PROCEDURE, TRY, CATCH
    }

    private LinkedList<TreeNode> scopes = new LinkedList<>();


    private String engineName;

    public String getEngineName() {
        return engineName;
    }

    public void setEngineName(String engineName) {
        this.engineName = engineName;
    }
//    private static class SessionHolder {
//        private final static ExecSession session = new ExecSession();
//    }

    public ExecSession(SparkSession sparkSession) {
        rootNode = new RootNode();
//        exceptions = new ArrayList<Exception>();
        resultSets = new ArrayList<>();
        this.sparkSession = sparkSession;
        String sparkDb = sparkSession.catalog().currentDatabase();
        if (StringUtils.isNotBlank(sparkDb)) {
            this.database = sparkDb;
        }
    }

    public void bindVariableContainer() {
        variableContainer = new VariableContainer(this);
    }

//    public static ExecSession getSession() {
//        return SessionHolder.session;

//    }


    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public void addLogicalPlans(LogicalPlan plan) {
        logicalPlans.add(plan);
    }

    public List<LogicalPlan> getLogicalPlans() {
        return logicalPlans;
    }

    public boolean isReset() {
        return isReset;
    }

    public void setReset(boolean reset) {
        isReset = reset;
    }

    public void setVariableContainer(VariableContainer variableContainer) {
        this.variableContainer = variableContainer;
    }

    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public void setSparkSession(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public void addRs(ResultSet rs) {
        resultSets.add(rs);
    }

//    public void addException(Exception exception) {
//        exceptions.add(exception);
//    }

    public void setVisitor(AbstractParseTreeVisitor visitor) {
        this.visitor = visitor;
    }

    public VariableContainer getVariableContainer() {
        return variableContainer;
    }

    public TreeNode getRootNode() {
        return rootNode;
    }

    public List<ResultSet> getResultSets() {
        return resultSets;
    }

    public void setErrorStr(String str) {
        errorStr = str;
    }

    public String getErrorStr() {
        return errorStr;
    }


    public String getRealTableName(String tableName) throws Exception {
        TmpTableNameUtils tmpTableNameUtils = new TmpTableNameUtils();
        String realTableName = "";
        if (tableName.indexOf("@") != -1) {
            realTableName = getVariableContainer().findTableVarAlias(tableName);
        } else if (tmpTableNameUtils.checkIsTmpTable(tableName)) {
            if(tableName.contains(".")){
                tableName = tableName.split("\\.")[1];
            }
            realTableName = sparkSession.getRealTable(tableName);
            if (StringUtils.equals(tableName, realTableName)) {
                realTableName = tmpTableNameUtils.createTableName(tableName);
                sparkSession.addTableToSparkSeesion(tableName, realTableName, 2);
            }
        } else if (tmpTableNameUtils.checkIsGlobalTmpTable(tableName)) {
            if(tableName.contains(".")){
                tableName = tableName.split("\\.")[1];
            }
            realTableName = tmpTableNameUtils.getGlobalTbName(tableName);
        } else {
            realTableName = tableName;
        }
        if (StringUtils.isBlank(realTableName)) {
            throw new Exception("Table " + tableName + " is not  exist ");
        }
        return realTableName;
    }

    public void enterScope(TreeNode node) {
        scopes.add(node);
    }

    public void leaveScope() {
        scopes.pollLast();
    }

    public TreeNode getCurrentScope() {
        if (scopes.isEmpty())
            return null;
        return scopes.getLast();
    }

    public TreeNode[] getCurrentScopes() {
        if (scopes.isEmpty())
            return new TreeNode[0];
        Object[] nodes = scopes.toArray();
        List<Object> tmpList = Arrays.asList(nodes);
        Collections.reverse(tmpList);
        TreeNode[] scopesArray = new TreeNode[tmpList.size()];
        tmpList.toArray(scopesArray);
        return scopesArray;
    }

    public ConcurrentHashMap<String, ConcurrentHashMap<String, Var>> getPackageVars() {
        return variableContainer.getPackageVars();
    }

}
