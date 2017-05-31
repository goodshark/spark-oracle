package org.apache.hive.tsql.cfl;

import org.apache.hive.basesql.cfl.CommonReturnStatement;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;

import org.apache.hive.tsql.common.TreeNode;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class ReturnStatement extends CommonReturnStatement {
    public ReturnStatement() {
    }

    public ReturnStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    @Override
    public void postExecute(Var resVar) throws Exception {
        if (resVar == null) {
            getExecSession().getVariableContainer().setReturnVal(0);
            return;
        }
        int result = 0;
        if (resVar == null || resVar.getDataType() == Var.DataType.NULL) {
            System.out.println("return default 0");
        } else if (resVar.getDataType().equals(Var.DataType.INT)) {
            result = Integer.parseInt(resVar.getVarValue().toString());
        } else {
            try {
                result = Integer.parseInt(resVar.getVarValue().toString());
            } catch (Exception e) {
                throw new Exception("RETURN transform integer failed");
            }
        }
        getExecSession().getVariableContainer().setReturnVal(result);
    }

    public BaseStatement createStatement() {
        return null;
    }
}
