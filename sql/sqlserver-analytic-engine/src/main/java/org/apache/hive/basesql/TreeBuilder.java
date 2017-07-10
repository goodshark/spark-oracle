package org.apache.hive.basesql;

import org.antlr.v4.runtime.ParserRuleContext;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.exception.Position;
import org.apache.hive.tsql.exception.UnsupportedException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by dengrb1 on 3/31 0031.
 */
public class TreeBuilder {
    private TreeNode rootNode;
    private LinkedList<TreeNode> curNodeStack = new LinkedList<TreeNode>();

    private List<Exception> exceptions;

    public TreeBuilder() {
    }

    public TreeBuilder(TreeNode root) {
        rootNode = root;
        exceptions = new ArrayList<>();
    }

    private Position locate(ParserRuleContext ctx) {
        return new Position(ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine(),
                ctx.getStop().getLine(), ctx.getStop().getCharPositionInLine());
    }

    public void addException(String msg, ParserRuleContext ctx) {
        exceptions.add(new UnsupportedException(msg, locate(ctx)));
    }

    public void addException(String msg, Position position) {
        exceptions.add(new UnsupportedException(msg, position));
    }

    public void addException(Exception exception) {
        exceptions.add(exception);
    }

    public List<Exception> getExceptions() {
        return exceptions;
    }

    public TreeNode getRootNode() {
        return rootNode;
    }

    public void addNode(TreeNode pNode) {
        Iterator<TreeNode> iter = curNodeStack.iterator();
        while (iter.hasNext()) {
            TreeNode node = this.popStatement();
            if (null == node) {
                continue;
            }
            pNode.addNode(node);
        }
    }

    public void addNodeAndPush(TreeNode pNode) {
        addNode(pNode);
        pushStatement(pNode);
    }

    public List<TreeNode> popAll() {
        List<TreeNode> children = new ArrayList<TreeNode>();
        Iterator<TreeNode> iter = curNodeStack.iterator();
        while (iter.hasNext()) {
            children.add(popStatement());
        }
        return children;
    }

    public TreeNode popStatement() {
        if (this.curNodeStack.isEmpty()) {
            return null;
        }
        return this.curNodeStack.pollFirst();
    }

    public TreeNode pushStatement(TreeNode treeNode) {
        return this.curNodeStack.offerLast(treeNode) ? treeNode : null;
    }
}
