package org.apache.hive.tsql.dml;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.common.TreeNode;

/**
 * Created by wangsm9 on 2016/12/29.
 */
public class LimitStatement extends SqlStatement {

    private TreeNode limitValueNode;

    @Override
    public int execute() throws Exception {
        limitValueNode.setExecSession(getExecSession());
        limitValueNode.execute();
        if (this.getNodeType().equals(Type.LIMIT_NUMBER)) {
            Var var = (Var) limitValueNode.getRs().getObject(0);
            setSql(new StringBuffer().append(Common.LIMIT).append(var.getVarValue().toString()).toString());
        } else {
            //TODO 百分比的计算，先计算count
        }
        return 1;
    }

    public void setLimitValueNode(TreeNode limitValueNode) {
        this.limitValueNode = limitValueNode;
    }

}
