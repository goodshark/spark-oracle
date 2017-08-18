package org.apache.hive.plsql.expression;

import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dml.ExpressionStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 8/18 0018.
 */
public class RelationUnaryExpression extends ExpressionStatement {
    private String action;
    private TreeNode queryNode;
    private List<TreeNode> exprs = new ArrayList<>();

    public RelationUnaryExpression() {
    }

    public void setAction(String str) {
        action = str;
    }

    public void setQuery(TreeNode n) {
        queryNode = n;
    }

    public void addExpr(TreeNode n) {
        exprs.add(n);
    }

    @Override
    public int execute() throws Exception {
        // SOME | EXISTS | ALL | ANY
        return 0;
    }

    @Override
    public String getOriginalSql() {
        StringBuilder sb = new StringBuilder();
        sb.append(action).append(" ( ");
        if (queryNode != null) {
            queryNode.setExecSession(getExecSession());
            sb.append(queryNode.getOriginalSql());
        } else {
            for (int i = 0; i < exprs.size(); i++) {
                exprs.get(i).setExecSession(getExecSession());
                sb.append(exprs.get(i).getOriginalSql());
                if (i < exprs.size() - 1)
                    sb.append(", ");
            }
        }
        sb.append(" ) ");
        return sb.toString();
    }

    @Override
    public String getFinalSql() throws Exception {
        StringBuilder sb = new StringBuilder();
        sb.append(action).append(" ( ");
        if (queryNode != null) {
            queryNode.setExecSession(getExecSession());
            sb.append(queryNode.getFinalSql());
        } else {
            for (int i = 0; i < exprs.size(); i++) {
                exprs.get(i).setExecSession(getExecSession());
                sb.append(exprs.get(i).getFinalSql());
                if (i < exprs.size() - 1)
                    sb.append(", ");
            }
        }
        sb.append(" ) ");
        return sb.toString();
    }
}
