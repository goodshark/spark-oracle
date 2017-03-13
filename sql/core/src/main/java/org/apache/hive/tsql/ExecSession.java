package org.apache.hive.tsql;

import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;
import org.apache.hive.tsql.arg.VariableContainer;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.RootNode;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.SparkPlan;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

/**
 * Created by zhongdg1 on 2016/11/29.
 */
public class ExecSession {


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
    // mark break/continue/goto/return/raise/throw cmd
    public enum Scope {
        BEGIN, IF, WHILE, PROCEDURE, TRY, CATCH
    }

//    private static class SessionHolder {
//        private final static ExecSession session = new ExecSession();
//    }

    public ExecSession() {
        rootNode = new RootNode();
//        exceptions = new ArrayList<Exception>();
        this.variableContainer = new VariableContainer();
        resultSets = new ArrayList<>();
    }

//    public static ExecSession getSession() {
//        return SessionHolder.session;

//    }

    public void addLogicalPlans(LogicalPlan plan){
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
}
