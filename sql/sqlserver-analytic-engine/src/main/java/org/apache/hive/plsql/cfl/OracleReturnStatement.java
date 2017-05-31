package org.apache.hive.plsql.cfl;

import org.apache.hive.basesql.cfl.CommonReturnStatement;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

/**
 * Created by dengrb1 on 5/25 0025.
 */
public class OracleReturnStatement extends CommonReturnStatement {

    public OracleReturnStatement() {
    }

    public OracleReturnStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    @Override
    public void postExecute(Var resVar) throws Exception {
        if (resVar == null)
            return;
        getExecSession().getVariableContainer().setFuncReturnVar(resVar);
    }

    public BaseStatement createStatement() {
        return null;
    }
}
