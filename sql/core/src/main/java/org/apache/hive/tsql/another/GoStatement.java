package org.apache.hive.tsql.another;

import org.apache.hive.tsql.cfl.GotoStatement;
import org.apache.hive.tsql.cfl.ThrowStatement;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.ddl.CreateProcedureStatement;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by zhongdg1 on 2016/12/12.
 */
public class GoStatement extends BaseStatement {

    private static final String STATEMENT_NAME = "_GO_";
    private int repeat = 1;
    // indicate goto in which GO block
    static private int goSeq = 0;
    // statement that location sensitive, include: CREATE PROC/THROW without args/GOTO
    private List<CreateProcedureStatement> procList = new ArrayList<CreateProcedureStatement>();
    private List<ThrowStatement> throwList = new ArrayList<ThrowStatement>();
    private List<GotoStatement> gotoActionList = new ArrayList<GotoStatement>();
    private List<GotoStatement> gotoLabelList = new ArrayList<GotoStatement>();

    public GoStatement() {
        super(STATEMENT_NAME);
        setNodeType(TreeNode.Type.GO);
        goSeq += 1;
    }

    public void setRepeat(int n) {
        repeat = n;
    }

    public int getRepeat() {
        return repeat;
    }

    public void decRepeat() {
        repeat = repeat - 1;
    }

    @Override
    public int execute() throws Exception {
        Set<String> tableNames = new HashSet<>();
        for (String tableName : getExecSession().getVariableContainer().getAllTableVarNames()) {
            tableNames.add(findTableVarAlias(tableName));
        }
//        for (String tmpTableName : getExecSession().getVariableContainer().getAllTmpTableNames()) {
//            if (isLocalTmpTable(tmpTableName)) {
//                tableNames.add(findTmpTaleAlias(tmpTableName));
//                getExecSession().getVariableContainer().deleteTmpTable(tmpTableName);
//            }
//        }
        for (String tableName : tableNames) {
            StringBuffer sb = new StringBuffer();
            sb.append("DROP TABLE ").append(tableName);
            commitStatement(sb.toString());
        }
        if (getExecSession().isReset()) {
            super.getExecSession().getVariableContainer().resetVars();
        }


        return 0;
    }

    private boolean isLocalTmpTable(String tmpTableName) {
        return '#' == tmpTableName.charAt(0) && '#' != tmpTableName.charAt(1);
    }

    @Override
    public BaseStatement createStatement() {
        return this;
    }

    public void addCreateProcStmt(CreateProcedureStatement stmt) {
        procList.add(stmt);
    }

    public void addThrowStmt(ThrowStatement stmt) {
        throwList.add(stmt);
    }

    public void addGotoAction(GotoStatement stmt) {
        gotoActionList.add(stmt);
    }

    public void addGotoLabel(GotoStatement stmt) {
        gotoLabelList.add(stmt);
    }

    public List<CreateProcedureStatement> getProcList() {
        return procList;
    }

    public List<ThrowStatement> getThrowList() {
        return throwList;
    }

    public List<GotoStatement> getGotoActionList() {
        return gotoActionList;
    }

    public List<GotoStatement> getGotoLabelList() {
        return gotoLabelList;
    }

    public String getGoSeq() {
        return "GO" + Integer.toString(goSeq) + "-";
    }
}
