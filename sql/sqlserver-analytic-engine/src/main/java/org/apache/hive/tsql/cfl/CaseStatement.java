package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.node.LogicNode;

/**
 * Created by chenfl2 on 2017/7/12.
 */
public class CaseStatement extends BaseStatement {

    private String switchVar = null;
    private boolean isSimple = true;

    public CaseStatement(boolean isSimple) {
        super();
        this.isSimple = isSimple;
    }

    public CaseStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void setSwitchVar(String node) {
        switchVar = node;
    }

    public String getSwitchVar() {return switchVar;}

    public boolean isSimple(){return isSimple;}

    public int execute() {
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }
}
