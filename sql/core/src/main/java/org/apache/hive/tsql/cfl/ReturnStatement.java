package hive.tsql.cfl;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

import java.sql.ResultSet;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class ReturnStatement extends BaseStatement {
    private TreeNode expr = null;
    public ReturnStatement() {
    }

    public ReturnStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public int execute() throws Exception {
        if (expr == null) {
            getExecSession().getVariableContainer().setReturnVal(0);
            return 0;
        }
        expr.setExecSession(getExecSession());
        expr.execute();
        ResultSet exprRes = expr.getRs();
        try {
            int result = 0;
            Var exprVar = (Var) exprRes.getObject(0);
            if (exprVar == null || exprVar.getDataType() == Var.DataType.NULL) {
                System.out.println("return default 0");
            } else if (exprVar.getDataType().equals(Var.DataType.INT)) {
                result = Integer.parseInt(exprVar.getVarValue().toString());
            } else {
                try {
                    result = Integer.parseInt(exprVar.getVarValue().toString());
                } catch (Exception e) {
                    throw new Exception("RETURN transform integer failed");
                }
            }
            getExecSession().getVariableContainer().setReturnVal(result);
        } catch (Exception e) {
            System.out.println("return error");
            e.printStackTrace();
            throw e;
        }
        return 0;
    }

    public void setExpr(TreeNode e) {
        expr = e;
    }

    public BaseStatement createStatement() {
        return null;
    }
}
