package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.node.LogicNode;
import org.apache.hive.tsql.common.TreeNode;

import java.util.*;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class WhileStatement extends BaseStatement {

    private LogicNode condtionNode = null;
    // store index variables in loop: for i in 1..10
    private Map<String, Var> varMap = new HashMap<>();
    // store labels with while
    private Set<String> labels = new HashSet<>();
    // check labels belong to while is searched
    private boolean labelSearched = false;

    public WhileStatement() {
    }

    public WhileStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void setCondtionNode(LogicNode node) {
        condtionNode = node;
    }

    public LogicNode getCondtionNode() {
        return condtionNode;
    }

    public boolean isTrue() throws Exception {
        if (condtionNode != null) {
            condtionNode.setExecSession(getExecSession());
            condtionNode.execute();
            return condtionNode.getBool();
        } else {
            return false;
        }
    }

    public int execute() throws Exception {
        return 0;
    }

    public void setLabelSearched() {
        labelSearched = true;
    }

    public boolean isLabelSearched() {
        return labelSearched;
    }

    public boolean existLabel(String name) {
        return labels.contains(name);
    }

    public void searchAllLabels() {
        TreeNode pNode = getParentNode();
        List<TreeNode> childList = pNode.getChildrenNodes();
        // label always before whileStmt
        for (TreeNode child: childList) {
            if (child.equals(this))
                break;
            else {
                if (child.getNodeType() == TreeNode.Type.GOTO) {
                    GotoStatement gotoStatement = (GotoStatement) child;
                    if (!gotoStatement.getAction()) {
                        labels.add(gotoStatement.getLabel());
                    }
                }
            }
        }
        labelSearched = true;
    }

    public void setLoopIndexVar(Var index) {
        varMap.put(index.getVarName(), index);
    }

    public BaseStatement createStatement() {
        return null;
    }
}
