package org.apache.hive.plsql.block;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dengrb1 on 4/1 0001.
 */
public class AnonymousBlock extends BaseStatement {
    private List<TreeNode> executeList = new ArrayList<>();
    private List<TreeNode> exceptionList = new ArrayList<>();


    public AnonymousBlock(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void addExecuteNode(TreeNode node) {
        executeList.add(node);
    }

    public void addExecptionNode(TreeNode node) {
        exceptionList.add(node);
    }

    public int execute() throws Exception {
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }
}
