package org.apache.hive.tsql.node;

import org.apache.hive.tsql.common.TreeNode;

import java.util.List;

/**
 * Created by dengrb1 on 12/5 0005.
 */
public class LogicNode extends TreeNode {
    private boolean notFlag = false;
    private String logicStr = null;

    private boolean priority = false;

    private boolean boolFlag = false;

    public LogicNode() {
        super();
    }

    public LogicNode(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void setPriority() {
        priority = true;
    }

    public boolean isPriority() {
        return priority;
    }

    public void setNot() {
        notFlag = true;
    }

    public boolean getNot() {
        return notFlag;
    }

    public boolean getBool() {
        return boolFlag;
    }

    public void setBool(boolean bool) {
        boolFlag = bool;
    }

    @Override
    public int execute() throws Exception {
        List<TreeNode> list = getChildrenNodes();
        if (getNodeType() == Type.OR) {
            executeOr(list, true);
        } else if (getNodeType() == Type.AND) {
            executeAnd(list, true);
        } else if (getNodeType() == Type.NOT) {
            executeNot(list, true);
        }
        return 0;
    }

    private int executeOr(List<TreeNode> list, boolean exec) throws Exception {
        if (list.size() != 2)
            return -1;
        LogicNode left = (LogicNode) list.get(0);
        LogicNode right = (LogicNode) list.get(1);

        if (!exec) {
            logicStr = left.toString() + " or " + right.toString();
            return 0;
        }

        left.execute();
        right.execute();
        if (left.getBool() || right.getBool()) {
            setBool(true);
            return 0;
        } else {
            setBool(false);
            return 0;
        }
    }

    private int executeAnd(List<TreeNode> list, boolean exec) throws Exception{
        if (list.size() != 2)
            return -1;
        LogicNode left = (LogicNode) list.get(0);
        LogicNode right = (LogicNode) list.get(1);

        if (!exec) {
            logicStr = left.toString() + " and " + right.toString();
            return 0;
        }

        left.execute();
        right.execute();
        if (left.getBool() && right.getBool()) {
            setBool(true);
            return 0;
        } else {
            setBool(false);
            return 0;
        }
    }

    private int executeNot(List<TreeNode> list, boolean exec)  throws Exception {
        if (list.size() != 1)
            return -1;
        LogicNode node = (LogicNode) list.get(0);
        String notStr = node.getNot() ? "NOT " : "";

        if (!exec) {
            logicStr = notStr + node.toString();
            return 0;
        }

        node.execute();
        if (getNot()) {
            setBool(!node.getBool());
            return 0;
        } else {
            setBool(node.getBool());
            return 0;
        }
    }

    public String toString() {
        try {
            List<TreeNode> list = getChildrenNodes();
            if (getNodeType() == Type.OR) {
                executeOr(list, false);
            } else if (getNodeType() == Type.AND) {
                executeAnd(list, false);
            } else if (getNodeType() == Type.NOT) {
                executeNot(list, false);
            }
            if (isPriority())
                logicStr = "(" + logicStr + ")";
            return logicStr;
        } catch (Exception e) {
            e.printStackTrace();
        }
        /*if (logicStr != null)
            return logicStr;*/
        return logicStr;
    }
}
