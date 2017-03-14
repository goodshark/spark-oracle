package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.util.StrUtils;

import java.sql.ResultSet;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class PrintStatement extends BaseStatement {
    private TreeNode expr = null;

    public PrintStatement() {
    }

    public PrintStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void addExpression(TreeNode cmd) {
        expr = cmd;
    }

    public int execute() throws Exception {
        expr.setExecSession(getExecSession());
        if (expr == null)
            return -1;
        expr.execute();
        ResultSet exprRes = expr.getRs();
        try {
            Var exprVar = (Var) exprRes.getObject(0);
            if (exprVar == null || exprVar.getDataType() == Var.DataType.NULL || null ==  exprVar.getVarValue()) {
                System.out.println("print result:");
                System.out.flush();
            } else {
                System.out.println("print result: " + StrUtils.trimQuot(exprVar.toString()));
                System.out.flush();
            }
        } catch (Exception e) {
            System.out.println("print error");
            e.printStackTrace();
            throw e;
        }
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }
}
