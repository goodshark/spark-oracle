package org.apache.hive.tsql.execute;

import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.another.GoStatement;
import org.apache.hive.tsql.cfl.*;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.exception.UnsupportedException;
import org.apache.spark.sql.catalyst.plans.logical.Except;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    public Executor(ExecSession session) {
        this(session,null);
    }

    public Executor(ExecSession session, TreeNode currentNode) {
        this.session = session;
        this.currentNode = null == currentNode ? session.getRootNode() : currentNode; //如果没有指定当前节点，则从跟节点开始执行
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
                    breakExecute();
                    break;
                case CONTINUE:
                    continueExecute();
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
                    goExecute(node);
                    break;
                default:
                    node.execute();
                    pushChild(node);
            }
        } catch (Exception e) {
            // throw statement exception, if stmt in try-catch, handle it
            ThrowStatement throwStmt = new ThrowStatement(TreeNode.Type.THROW);
            throwStmt.setMsg(e.getMessage());
            // TODO make error number map error msg
            throwStmt.setStateNumStr("200");
            throwStmt.setErrorNumStr("60000");
            throwExecute(throwStmt);
        }
    }

    public void pushChild(TreeNode node) {
        List<TreeNode> list = node.getChildrenNodes();
        for (int i = list.size() - 1; i >= 0; i--)
            stack.push(list.get(i));
    }

    public void goExecute(TreeNode node) throws Exception {
        // check procedure in GO-block
        LOG.info("sql-server executor check GO block");
        boolean pass = procCheck(node);
        if (!pass) {
            LOG.error("sql-server executor check procedure node failed");
            throw new Exception("create procedure should be the 1st in GO block");
        }
        node.execute();
        GoStatement goNode = (GoStatement) node;
        LOG.info("sql-server executor check GO block all ok, start execute GO block, repeat number: " + goNode.getRepeat());
        goNode.decRepeat();
        if (goNode.getRepeat() >= 1)
            stack.push(goNode);
        pushChild(node);
    }

    public boolean procCheck(TreeNode node) {
        if (node != null) {
            if (node.getNodeName() == "_CSP_") {
                TreeNode pNode = node.getParentNode();
                if (pNode != null) {
                    List<TreeNode> sqlClauesChilds = pNode.getChildrenNodes();
                    if (sqlClauesChilds.isEmpty() || !sqlClauesChilds.get(0).equals(node))
                        return false;
                    TreeNode ppNode = pNode.getParentNode();
                    if (ppNode == null || ppNode.getNodeType() != TreeNode.Type.GO)
                        return false;
                    else
                        return true;
                } else {
                    return false;
                }
            } else {
                List<TreeNode> childList = node.getChildrenNodes();
                for (TreeNode child: childList) {
                    boolean flag = procCheck(child);
                    if (!flag)
                        return flag;
                }
                return true;
            }
        } else {
            return true;
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
        WhileStatement whileStmt = (WhileStatement) node;
        List<TreeNode> list = whileStmt.getChildrenNodes();
        if (list.size() != 1)
            return;
        TreeNode whileBlock = list.get(0);
        if (whileStmt.isTrue()) {
            stack.push(node);
            stack.push(whileBlock);
        }
    }

    public void breakExecute() throws Exception {
        TreeNode stmt = null;
        while (!stack.empty()) {
            stmt = stack.pop();
            if (stmt.getNodeType() == TreeNode.Type.WHILE)
                break;
        }
        if (stmt == null || stmt.getNodeType() != TreeNode.Type.WHILE)
            throw new UnsupportedException("BREAK stmt is not in WHILE block");
    }

    public void continueExecute() throws Exception {
        TreeNode stmt = null;
        while (!stack.empty()) {
            stmt = stack.pop();
            if (stmt.getNodeType() == TreeNode.Type.WHILE) {
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
        // check if goto-action or goto-label in try-catch block
        TreeNode tryCatchNode1 = findTryAncestorNode(node);
        TreeNode tryCatchNode2 = findTryAncestorNode(labelNode);
        if (!isGotoLegal(tryCatchNode1, tryCatchNode2))
            throw new UnsupportedException("GOTO can not jump into TRY-CATCH block");
        buildReverseStack(labelNode);
        rebuildStack();
    }

    public TreeNode findGotoLabel(String label, TreeNode node) {
        if (node != null) {
            if (node.getNodeType() == TreeNode.Type.GOTO) {
                GotoStatement stmt = (GotoStatement) node;
                if (!stmt.getAction() && stmt.getLabel().equals(label))
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

    private TreeNode findTryAncestorNode(TreeNode node) {
        if (node == null || node.getParentNode() == null)
            return null;
        TreeNode pNode = node.getParentNode();
        if (pNode.getNodeType() == TreeNode.Type.TRY)
            return pNode;
        else
            return findTryAncestorNode(pNode);
    }

    private boolean isGotoLegal(TreeNode node1, TreeNode node2) {
        if (node1 == null && node2 == null)
            return true;
        if (node1 != null && node2 == null)
            return true;
        if (node1 == null && node2 != null)
            return false;
        if (node1 != null && node2 != null && node1.equals(node2))
            return true;
        else
            return false;
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
        // get exception info
        exceptionNode.execute();
        // get catch block if there exists try-catch block
        TreeNode node = null;
        while (!stack.empty()) {
            node = stack.pop();
            if (node.getNodeType() == TreeNode.Type.TRY) {
                List<TreeNode> list = node.getChildrenNodes();
                if (list.size() != 2)
                    break;
                TreeNode catchStmt = list.get(1);
                stack.push(catchStmt);
                break;
            }
        }
        // exception catched by TRY-CATCH block
        if (node != null && node.getNodeType() == TreeNode.Type.TRY)
            return;
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
}
