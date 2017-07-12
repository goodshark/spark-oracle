package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dml.ExpressionStatement;

/**
 * Created by chenfl2 on 2017/7/12.
 */
public class CaseWhenStatement extends BaseStatement{

    private TreeNode whenCondition;

    private boolean isSimple = true;

    public CaseWhenStatement(boolean isSimple) {
        super();
        this.isSimple = isSimple;
    }

    public CaseWhenStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void setCondtion(TreeNode node) {
        whenCondition = node;
    }

    public TreeNode getCondition() {return whenCondition;}

    public boolean isSimple(){return isSimple;}

    public int execute() {
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }

}
