package org.apache.hive.tsql.execute;

import org.apache.hive.plsql.block.BlockBorder;
import org.apache.hive.plsql.cfl.OracleRaiseStatement;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.another.GoStatement;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.cfl.*;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.ddl.CreateProcedureStatement;
import org.apache.hive.tsql.exception.UnsupportedException;
import org.apache.hive.tsql.node.LogicNode;
import org.apache.spark.sql.catalyst.plans.logical.Except;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Stack;

/**
 * Created by zhongdg1 on 2016/12/20.
 * 遍历Tree
 */
public class Executor {
    private static final Logger LOG = LoggerFactory.getLogger(Executor.class);
    private ExecSession session;
    private TreeNode currentNode;
    private Stack<TreeNode> stack = new Stack<TreeNode>();
    private Stack<TreeNode> reverseStack = new Stack<TreeNode>();
    private String engine = "oracle";

    public Executor(ExecSession session) {
        this(session,null);
    }

    public Executor(ExecSession session, TreeNode currentNode) {
        this.session = session;
        this.currentNode = null == currentNode ? session.getRootNode() : currentNode; //如果没有指定当前节点，则从跟节点开始执行
        //engine = session.getSparkSession().conf().get("spark.sql.analytical.engine");
    }
    public void execute() throws Exception {
        if(null == this.currentNode) {
            return;
        }
//        this.currentNode.setExecSession(session);
        currentNode.execute();
        if(currentNode.isLeaf() || currentNode.isAtomic()) {
            return;
        }
        List<TreeNode> children = this.currentNode.getChildrenNodes();
        for(TreeNode node : children) {
            this.currentNode = node;
            execute();
        }
    }

    public void run() throws Exception {
        LOG.info("sql-server executor start execute");
        if (currentNode == null)
            return;
        currentNode.setExecSession(session);
        stack.add(currentNode);

        while (!stack.empty()) {
            TreeNode node = stack.pop();
            // TODO mark for GO/TRY
            node.setExecSession(session);
            if (node.isSkipable())
                continue;
            executeStmt(node);
        }
    }

    public void executeStmt(TreeNode node) throws Exception {
        try {
            switch (node.getNodeType()) {
                case IF:
                    ifExecute(node);
                    break;
                case WHILE:
                    whileExecute(node);
                    break;
                case BREAK:
                    breakExecute(node);
                    break;
                case CONTINUE:
                    continueExecute(node);
                    break;
                case GOTO:
                    gotoExecute(node);
                    break;
                case RETURN:
                    returnExecute(node);
                    break;
                case TRY:
                    tryExecute(node);
                    break;
                case THROW:
                    throwExecute(node);
                    break;
                case RAISE:
                    raiseExecute(node);
                    break;
                case GO:
                    goBlockCheck(node);
                    goExecute(node);
                    break;
                case ANONY_BLOCK:
                    anonyBlockExecute(node);
                    break;
                case BORDER:
                    borderExecute();
                    break;
                case ORACLE_RAISE:
                    oracleRaiseExecute(node);
                    break;
                case PL_FUNCTION:
                    plFunctionExecute(node);
                    break;
                default:
                    node.execute();
                    pushChild(node);
            }
        } catch (Exception e) {
            // TODO create different oracle/sqlserver exception based on session conf
            // throw statement exception, if stmt in try-catch, handle it
            e.printStackTrace();
            handleRunTimeException(e);
            /*ThrowStatement throwStmt = new ThrowStatement(TreeNode.Type.THROW);
            throwStmt.setMsg(e.toString());
            // TODO make error number map error msg
            throwStmt.setStateNumStr("200");
            throwStmt.setErrorNumStr("60000");
            throwExecute(throwStmt);*/
        }
    }

    private void handleRunTimeException(Exception exception) throws Exception {
        if (engine.equalsIgnoreCase("sqlserver")) {
            ThrowStatement throwStmt = new ThrowStatement(TreeNode.Type.THROW);
            throwStmt.setMsg(exception.toString());
            // TODO make error number map error msg
            throwStmt.setStateNumStr("200");
            throwStmt.setErrorNumStr("60000");
            throwExecute(throwStmt);
        } else if (engine.equalsIgnoreCase("oracle")) {
            OracleRaiseStatement raiseStatement = new OracleRaiseStatement(TreeNode.Type.ORACLE_RAISE);
            raiseStatement.setRunTimeException();
            raiseStatement.setExceptionInfo(exception.toString());
            oracleRaiseExecute(raiseStatement);
        } else {
            LOG.warn("executor get unknown engine " + engine + ", ignore it");
        }
    }

    public void pushChild(TreeNode node) {
        List<TreeNode> list = node.getChildrenNodes();
        for (int i = list.size() - 1; i >= 0; i--)
            stack.push(list.get(i));
    }

    public void enterBlock(TreeNode node) {
        if (engine.equalsIgnoreCase("oracle"))
            session.enterScope(node);
    }

    public void leaveBlock() {
        session.leaveScope();
    }

    public void goExecute(TreeNode node) throws Exception {
        node.execute();
        GoStatement goNode = (GoStatement) node;
        LOG.info("sql-server executor start execute GO block, repeat number: " + goNode.getRepeat());
        goNode.decRepeat();
        if (goNode.getRepeat() >= 1)
            stack.push(goNode);
        pushChild(node);
    }

    private void goBlockCheck(TreeNode node) throws Exception {
        LOG.info("sql-server executor start check GO block");
        GoStatement goNode = (GoStatement) node;
        checkCreateProc(goNode.getProcList());
        checkThrow(goNode.getThrowList());
        checkGoto(goNode.getGotoActionList(), goNode.getGotoLabelList());
        LOG.info("sql-server executor check GO block success");
    }

    private void checkCreateProc(List<CreateProcedureStatement> procList) throws Exception {
        LOG.info("sql-server executor start check create procedure in GO block");
        for (CreateProcedureStatement CPStmt: procList) {
            TreeNode pNode = CPStmt.getParentNode();
            if (pNode != null) {
                List<TreeNode> sqlClauesChilds = pNode.getChildrenNodes();
                if (sqlClauesChilds.isEmpty() || !sqlClauesChilds.get(0).equals(CPStmt))
                    throw new Exception("create procedure should be the 1st in GO block");
                TreeNode ppNode = pNode.getParentNode();
                if (ppNode == null || ppNode.getNodeType() != TreeNode.Type.GO)
                    throw new Exception("create procedure should be the 1st in GO block");
            } else
                throw new Exception("create procedure should be the 1st in GO block");
        }
    }

    private void checkThrow(List<ThrowStatement> throwList) throws Exception {
        LOG.info("sql-server executor start check throw without args in GO block");
        for (ThrowStatement throwNode: throwList) {
            TreeNode catchNode = findAncestorCatch(throwNode);
            if (catchNode == null)
                throw new Exception("throw without args can only exists in CATCH block");
        }
    }

    private TreeNode findAncestorCatch(TreeNode node) {
        if (node != null) {
            TreeNode pNode = node.getParentNode();
            if (pNode == null) {
                return null;
            } else {
                if (pNode.getNodeType() == TreeNode.Type.TRY) {
                    List<TreeNode> childList = pNode.getChildrenNodes();
                    if (childList.size() == 2 && node.equals(childList.get(1)))
                        return pNode;
                    else {
                        return null;
                    }
                } else {
                    return findAncestorCatch(pNode);
                }
            }
        } else {
            return null;
        }
    }

    private void checkGoto(List<GotoStatement> actionList, List<GotoStatement> labelList) throws Exception {
        LOG.info("sql-server executor start check goto in GO block");
        // check all label exists
        HashSet<String> labelSet = new HashSet<String>();
        for (GotoStatement labelNode: labelList) {
            if (labelSet.contains(labelNode.getLabel().toUpperCase()))
                throw new Exception("GOTO reference label " + labelNode.getLabel() + " that appear more than twice");
            else
                labelSet.add(labelNode.getLabel().toUpperCase());
        }
        for (GotoStatement actionNode: actionList) {
            if (!labelSet.contains(actionNode.getLabel().toUpperCase()))
                throw new Exception("GOTO reference label " + actionNode.getLabel() + " that NOT exists");
        }
        // check goto location sensitive(try-catch)
        for (GotoStatement labelNode: labelList) {
            for (GotoStatement actionNode: actionList) {
                TryCatchStatement tryCatchNode1 = (TryCatchStatement) findTryAncestorNode(actionNode);
                actionNode.setTryCatchNode(tryCatchNode1);
                if (tryCatchNode1 != null && tryCatchNode1.isCatchBlock()) {
                    actionNode.setBelongCatchBlock();
                    tryCatchNode1.clearBlockStatus();
                }
                TryCatchStatement tryCatchNode2 = (TryCatchStatement) findTryAncestorNode(labelNode);
                labelNode.setTryCatchNode(tryCatchNode2);
                if (tryCatchNode2 != null && tryCatchNode2.isCatchBlock()) {
                    labelNode.setBelongCatchBlock();
                    tryCatchNode2.clearBlockStatus();
                }
                if (!isGotoLegal(actionNode, labelNode))
                    throw new Exception("GOTO action and label are not in the same block (TRY-CATCH)");
            }
        }
    }

    public void ifExecute(TreeNode node) throws Exception {
        IfStatement ifStmt = (IfStatement) node;
        List<TreeNode> list = ifStmt.getChildrenNodes();
        if (list.size() < 1)
            return;
        TreeNode trueStmt = list.get(0);
        TreeNode falseStmt = list.size() == 2 ? list.get(1) : null;
        if (ifStmt.isTrue()) {
            stack.push(trueStmt);
        } else if (falseStmt != null) {
            stack.push(falseStmt);
        }
    }

    public void whileExecute(TreeNode node) throws Exception {
        // add while block border
        boolean initLoop = true;
        if (!stack.empty()) {
            TreeNode topNode = stack.peek();
            if (topNode.getNodeType() == TreeNode.Type.BORDER && ((BlockBorder) topNode).isSame(node)) {
                initLoop = false;
            }
        }
        if (initLoop) {
            enterBlock(node);
            stack.push(new BlockBorder(node));
            TreeNode conditionNode = ((WhileStatement) node).getCondtionNode();
            conditionNode.setExecSession(session);
            if (conditionNode instanceof LogicNode)
                ((LogicNode) conditionNode).initIndex();
        }

        WhileStatement whileStmt = (WhileStatement) node;
        List<TreeNode> list = whileStmt.getChildrenNodes();
        if (list.size() != 1)
            return;
        TreeNode whileBlock = list.get(0);
        if (whileStmt.isTrue()) {
            stack.push(node);
            stack.push(whileBlock);
        }
        /*// when enter into the same while-block more than twice, need re-init conditionNode
        // like FOR i IN names.FIRST .. names.LAST LOOP, when names has been changed before go into the second while-block
        whileStmt.refreshConditionNode();*/
    }

    public void breakExecute(TreeNode node) throws Exception {
        // exit when x > y
        node.execute();
        BreakStatement breakStatement = (BreakStatement) node;
        if (!breakStatement.isEnable()) {
            return;
        }

        TreeNode stmt = null;
        while (!stack.empty()) {
            stmt = stack.pop();
            // remember pop the scope
            if (stmt.getNodeType() == TreeNode.Type.BORDER)
                leaveBlock();
            if (stmt.getNodeType() == TreeNode.Type.WHILE && breakStatement.isPair(stmt))
                break;
        }
        if (stmt == null || stmt.getNodeType() != TreeNode.Type.WHILE)
            throw new UnsupportedException("BREAK stmt is not in WHILE block");
    }

    public void continueExecute(TreeNode node) throws Exception {
        // continue when x > y
        node.execute();
        ContinueStatement continueStatement = (ContinueStatement) node;
        if (!continueStatement.isEnable()) {
            return;
        }

        TreeNode stmt = null;
        while (!stack.empty()) {
            stmt = stack.pop();
            if (stmt.getNodeType() == TreeNode.Type.WHILE && continueStatement.isPair(stmt)) {
                stack.push(stmt);
                break;
            }
        }
        if (stmt == null || stmt.getNodeType() != TreeNode.Type.WHILE)
            throw new UnsupportedException("CONTINUE stmt is not in WHILE block");
    }


    public void gotoExecute(TreeNode node) throws Exception {
        GotoStatement gotoStmt = (GotoStatement) node;
        if (!gotoStmt.getAction()) {
            return;
        }
        while (!stack.empty()) {
            stack.pop();
        }
        TreeNode rootNode = session.getRootNode();
        TreeNode labelNode = findGotoLabel(gotoStmt.getLabel(), rootNode);
        buildReverseStack(labelNode);
        rebuildStack();
    }

    public TreeNode findGotoLabel(String label, TreeNode node) {
        if (node != null) {
            if (node.getNodeType() == TreeNode.Type.GOTO) {
                GotoStatement stmt = (GotoStatement) node;
                if (!stmt.getAction() && stmt.getLabel().equalsIgnoreCase(label))
                    return node;
            }
            List<TreeNode> list = node.getChildrenNodes();
            for (TreeNode child: list) {
                TreeNode target = findGotoLabel(label, child);
                if (target != null)
                    return target;
            }
            return null;
        } else {
            return null;
        }
    }

    public void buildReverseStack(TreeNode node) {
        if (node == null || node.getParentNode() == null)
            return;
        TreeNode pNode = node.getParentNode();
        if (pNode.getNodeType() == TreeNode.Type.WHILE || pNode.getNodeType() == TreeNode.Type.TRY) {
            reverseStack.push(pNode);
        } else if (pNode.getNodeType() != TreeNode.Type.IF) {
            List<TreeNode> list = pNode.getChildrenNodes();
            boolean flag = false;
            for (TreeNode child: list) {
                if (child.equals(node)) {
                    flag = true;
                    continue;
                }
                if (flag)
                    reverseStack.push(child);
            }
        }
        buildReverseStack(pNode);
    }

    public void rebuildStack() {
        while (!reverseStack.empty()) {
            TreeNode node = reverseStack.pop();
            stack.push(node);
        }
    }

    // null means outside goto, else means try-catch block goto
    private TreeNode findTryAncestorNode(TreeNode node) {
        if (node == null || node.getParentNode() == null)
            return null;
        TreeNode pNode = node.getParentNode();
        if (pNode.getNodeType() == TreeNode.Type.TRY) {
            List<TreeNode> childList = pNode.getChildrenNodes();
            if (childList.size() == 2) {
                if (childList.get(1).equals(node)) {
                    ((TryCatchStatement) pNode).setCatchBlock();
                }
            }
            return pNode;
        } else
            return findTryAncestorNode(pNode);
    }

    private boolean isGotoLegal(GotoStatement action, GotoStatement label) {
        if (action.getTryCatchNode() == null && label.getTryCatchNode() == null)
            return true;
        if (action.getTryCatchNode() != null && label.getTryCatchNode() == null)
            return true;
        if (action.getTryCatchNode() == null && label.getTryCatchNode() != null)
            return false;
        if (action.getTryCatchNode() != null && label.getTryCatchNode() != null) {
            if ((action.isBelongCatchBlock() && label.isBelongCatchBlock()) ||
                    (!action.isBelongCatchBlock() && !label.isBelongCatchBlock())) {
                return true;
            } else
                return false;
        }
        return true;
    }

    public void returnExecute(TreeNode returnNode) throws Exception {
        returnNode.execute();
        while (!stack.empty()) {
            TreeNode node = stack.pop();
            if (node.getNodeType() == TreeNode.Type.GO) {
                stack.push(node);
                break;
            }
        }
    }

    public void tryExecute(TreeNode tryCatchNode) {
        List<TreeNode> list = tryCatchNode.getChildrenNodes();
        if (list.size() != 2)
            return;
        TreeNode tryStmt = list.get(0);
        tryCatchNode.setSkipable(true);
        stack.push(tryCatchNode);
        stack.push(tryStmt);
    }

    public void raiseExecute(TreeNode node) throws Exception {
        throwExecute(node);
    }

    public void throwExecute(TreeNode exceptionNode) throws Exception {
        // get exception info, when THROW without args do not need execute
        if (!(exceptionNode.getNodeType() == TreeNode.Type.THROW && ((ThrowStatement)exceptionNode).isEmptyArg())) {
            exceptionNode.execute();
        } else {
            ((ThrowStatement)exceptionNode).setThrowExeceptionStr(session.getErrorStr());
        }
        // get catch block if there exists try-catch block
        TreeNode node = null;
        while (!stack.empty()) {
            node = stack.pop();
            // remember to pop scope
            if (node.getNodeType() == TreeNode.Type.BORDER)
                leaveBlock();
            if (node.getNodeType() == TreeNode.Type.TRY && node.isSkipable()) {
                List<TreeNode> list = node.getChildrenNodes();
                if (list.size() != 2)
                    break;
                TreeNode catchStmt = list.get(1);
                stack.push(catchStmt);
                break;
            }
        }
        // exception catched by TRY-CATCH block
        if (node != null && node.getNodeType() == TreeNode.Type.TRY) {
            String errorStr = "";
            // record this exception that maybe THROW without args in subsequent CATCH block
            if (exceptionNode.getNodeType() == TreeNode.Type.THROW) {
                errorStr = ((ThrowStatement) exceptionNode).getThrowExeceptionStr();
            } else {
                errorStr = ((RaiseStatement) exceptionNode).getExceptionStr();
            }
            session.setErrorStr(errorStr);
            return;
        }
        // exception is out of try-catch, just throw exception
        String expStr = "";
        if (exceptionNode.getNodeType() == TreeNode.Type.THROW) {
            expStr = ((ThrowStatement)exceptionNode).getThrowExeceptionStr();
            throw new Exception(expStr);
        } else {
            expStr = ((RaiseStatement)exceptionNode).getExceptionStr();
            throw new Exception(expStr);
        }
    }

    public void anonyBlockExecute(TreeNode node) throws Exception {
        // mark a anonymous block can be skipped for the raise statement
        // do not need mark skip, just need boarder node re-enter when meet raise stmt
//        node.setSkipable(true);
        stack.push(new BlockBorder(node));
        enterBlock(node);
        node.execute();
        pushChild(node);
    }

    public void preLeaveBlock() throws Exception {
        // when enter into the same while-block more than twice, need re-init conditionNode
        // like FOR i IN names.FIRST .. names.LAST LOOP, when names has been changed before go into the second while-block
        TreeNode block = session.getCurrentScope();
        if (block != null && block instanceof WhileStatement) {
                ((WhileStatement)block).refreshConditionNode();
        }
    }

    public void borderExecute() throws Exception {
        preLeaveBlock();
        leaveBlock();
    }

    public void oracleRaiseExecute(TreeNode exceptionNode) throws Exception {
        boolean nonExceptionHandler = true;
        OracleRaiseStatement raiseStatement = (OracleRaiseStatement) exceptionNode;
        String exceptionName = "";
        if (!raiseStatement.isRunTimeException()) {
            exceptionName = raiseStatement.getExceptionName();
            Var exceptionVar = session.getVariableContainer().findVar(exceptionName);
            if (exceptionVar == null)
                throw new Exception(exceptionName + " EXCEPTION is not declare");
        }

        TreeNode node = null;
        while(!stack.empty()) {
            node = stack.pop();
            if (node.getNodeType() == TreeNode.Type.BORDER) {
                BlockBorder blockBorder = (BlockBorder) node;
                if (blockBorder.checkException(exceptionName)) {
                    TreeNode exceptionBlock = blockBorder.getExceptionBlock(exceptionName);
                    if (exceptionBlock != null) {
                        // exception block still in anonymous block, when finish handling error then leave block
                        stack.push(node);
                        stack.push(exceptionBlock);
                        nonExceptionHandler = false;
                        break;
                    }
                } else {
                    leaveBlock();
                }
            }
        }

        if (nonExceptionHandler)
            throw new Exception(raiseStatement.getExceptionInfo());
    }

    public void plFunctionExecute(TreeNode node) throws Exception {
        node.execute();
    }
}
