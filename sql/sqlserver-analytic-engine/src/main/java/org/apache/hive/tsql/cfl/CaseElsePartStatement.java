package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

/**
 * Created by chenfl2 on 2017/7/12.
 */
public class CaseElseStatement extends BaseStatement{

    public CaseElseStatement() {
        super();
    }

    public CaseElseStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public int execute() {
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }

}
