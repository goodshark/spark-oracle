package org.apache.hive.tsql.common;

import org.apache.hive.basesql.func.CommonProcedureStatement;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.another.GoStatement;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.cursor.Cursor;
import org.apache.hive.tsql.func.Procedure;

import java.io.Serializable;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhongdg1 on 2016/11/29.
 */
public abstract class TreeNode implements Serializable {

    private static final long serialVersionUID = 1896477134640278241L;
    private int nodeId;
    private String nodeName;
    private int seqId;
    private List<TreeNode> childrenNodes = new ArrayList<TreeNode>();
    private TreeNode parentNode;
    private ResultSet rs;
    private boolean isAtomic = false; //是否是原子操作,true：则不会遍历children
    private boolean isSkipable = false; //Executor是否可以直接跳过
    boolean isExecutable = false; //Direct to sparkserver
    private ExecSession execSession;
    private String sql;
    private boolean addResult = false;
    private GoStatement goStmt = null;
    private boolean isCollectRs = true;

    public enum Type {
        IF, AND, OR, NOT, PREDICATE, WHILE, BREAK, CONTINUE, RETURN, GOTO, PRINT,
        RAISE, THROW, TRY, WAIT, BEGINEND, WHEN, THEN, ELSE, SWICH, CASE_INPUT, GO, TABLE_VALUE, DERIVED_TABLE,
        TABEL_DEFAULT_VALUES, EXECUTE_STATEMENT, LIMIT_NUMBER, LIMIT_PERCENT, CURSOR, DEFAULT, BORDER, ANONY_BLOCK
    }

    public boolean isCollectRs() {
        return isCollectRs;
    }

    public void setCollectRs(boolean collectRs) {
        isCollectRs = collectRs;
    }

    public boolean isAtomic() {
        return isAtomic;
    }

    public void setAddResult(boolean addResult) {
        this.addResult = addResult;
    }

    public boolean isAddResult() {
        return addResult;
    }

    public void setAtomic(boolean atomic) {
        isAtomic = atomic;
    }

    public boolean isSkipable() {
        return isSkipable;
    }

    public void setSkipable(boolean skipable) {
        isSkipable = skipable;
    }

    public void setExecutable(boolean executable) {
        isExecutable = executable;
    }

    public ResultSet getRs() {
        return rs;
    }

    public void setRs(ResultSet rs) {
        this.rs = rs;
    }

    private Type nodeType = null;

    public String getSql() {
        return sql;
    }

    public String getOriginalSql() {
        return "";
    }

    public String getFinalSql() throws Exception {
        return "";
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public TreeNode() {
//        System.out.println("fd");
//        this.execSession = ExecSession.getSession();
        setNodeType(Type.DEFAULT);
    }

    public TreeNode(int nodeId, String nodeName, int seqId) {
        this.nodeId = nodeId;
        this.nodeName = nodeName;
        this.seqId = seqId;
        setNodeType(Type.DEFAULT);
    }

    public TreeNode(String nodeName) {
        this();
        this.nodeName = nodeName;
        setNodeType(Type.DEFAULT);
    }

    public TreeNode getParentNode() {
        return parentNode;
    }

    public List<TreeNode> getChildrenNodes() {
        for (TreeNode child : childrenNodes) {
            child.setExecSession(execSession);
        }
        return childrenNodes;
    }

    public void setNodeId(int nodeId) {
        this.nodeId = nodeId;
    }

    public void setNodeName(String nodeName) {
        this.nodeName = nodeName;
    }

    public void setSeqId(int seqId) {
        this.seqId = seqId;
    }

    public Boolean isLeaf() {
        return this.childrenNodes.isEmpty();
    }

    public TreeNode addNode(TreeNode newNode) {
        if (null == newNode) {
            return null;
        }
        newNode.setNodeId(Sequence.getNodeId());
        newNode.setSeqId(childrenNodes.size());
        this.childrenNodes.add(newNode);
        newNode.parentNode = this;
        return newNode;
    }

    public TreeNode insertNode(int index, TreeNode newNode) {
        newNode.setNodeId(Sequence.getNodeId());
        if (index >= childrenNodes.size()) {
            newNode.setSeqId(childrenNodes.size());
            childrenNodes.add(newNode);
        } else {
            newNode.setSeqId(index);
            childrenNodes.add(index, newNode);
        }
        return newNode;
    }

    public void setNodeType(Type t) {
        nodeType = t;
    }

    public Type getNodeType() {
        return nodeType;
    }

    public String getNodeName() {
        return nodeName;
    }

    public ExecSession getExecSession() {
        return execSession;
    }

    public void setExecSession(ExecSession execSession) {
        this.execSession = execSession;
    }

    public void addVar(Var var) {
        this.execSession.getVariableContainer().addVar(var);
    }

    public Var findVar(String varName) {
        return this.execSession.getVariableContainer().findVar(varName);
    }

    public Var updateVarValue(String name, Object val) {
        return this.execSession.getVariableContainer().updateValue(name, val);
    }

    public void removeVar(String varName) {
        this.execSession.getVariableContainer().deleteVar(varName);
    }

    public void addFunc(CommonProcedureStatement function) {
        this.execSession.getVariableContainer().addProcFunc(function);
    }

    public CommonProcedureStatement findFunc(String funcName) {
        return this.execSession.getVariableContainer().findFunc(funcName);
    }

    public CommonProcedureStatement findFunc(String funcName, List<Var> procs) {
        return this.execSession.getVariableContainer().findFunc(funcName, procs);
    }

    /**
     * execute node
     *
     * @return
     */
    public abstract int execute() throws Exception;

    public Cursor findCursor(String name, boolean isGlobal) {
        return this.execSession.getVariableContainer().findCursor(name.trim().toUpperCase(), isGlobal);
    }

    public Cursor findCursor(String name) {
        return this.execSession.getVariableContainer().findCursor(name.trim().toUpperCase());
    }

    public void addCursor(Cursor cursor) {
        this.execSession.getVariableContainer().addCursor(cursor);
    }

    public void deleleCursor(String cursorName, boolean isGlobal) {
        this.execSession.getVariableContainer().deleteCursor(cursorName.trim().toUpperCase(), isGlobal);
    }

    public Var addOrUpdateSys(Var var) {
        return this.execSession.getVariableContainer().addOrUpdateSys(var);
    }

    public boolean updateSys(String sysVarName, Object val) {
        return this.execSession.getVariableContainer().updateSys(sysVarName, val);
    }

    public Var findSystemVar(String sysName) {
        return execSession.getVariableContainer().findSystemVar(sysName);
    }

    public void addTableVars(Var var) {
        this.execSession.getVariableContainer().addTableVars(var);
    }

    public String findTableVarAlias(String tableVarName) {
        return execSession.getVariableContainer().findTableVarAlias(tableVarName);
    }

    public void addTmpTable(String tmpTable, String aliasName) {
        execSession.getVariableContainer().addTmpTable(tmpTable, aliasName);
    }

    public String findTmpTaleAlias(String tmpTableName) {
        return execSession.getVariableContainer().findTmpTaleAlias(tmpTableName);
    }


    public Var getExpressionValue() throws Exception {
//        Var var = null;
//        try {
        return (Var) getRs().getObject(0);
//        }catch (SQLException e){
//            e.printStackTrace();
//        }
//        return var;
    }

    public void setCurrentGoStmt(GoStatement stmt) {
        goStmt = stmt;
    }

    public GoStatement currentGoStmt() {
        return goStmt;
    }


}