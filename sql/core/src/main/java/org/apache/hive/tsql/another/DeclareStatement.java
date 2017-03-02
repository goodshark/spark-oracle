package org.apache.hive.tsql.another;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.exception.AlreadyDeclaredException;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhongdg1 on 2016/12/1.
 */
public class DeclareStatement extends BaseStatement {
    private static final String STATEMENT_NAME = "_DECLARE_";
    private List<Var> declareVars = new ArrayList<>();

    public DeclareStatement() {
        super(STATEMENT_NAME);
    }

    public void addDeclareVar(Var var) {
        this.declareVars.add(var);
    }

    @Override
    public int execute() throws Exception {
        for (Var var : declareVars) {
//            if (null != findVar(var.getVarName())) {
//                throw new AlreadyDeclaredException(var.getVarName());
//            }
            switch (var.getValueType()) {
                case TABLE:
                    String aliasName = productAliasName(var.getVarName());
                    StringBuffer sb = new StringBuffer();
                    sb.append("CREATE TABLE ").append(aliasName).append("(").
                            append(var.getVarValue().toString()).append(")");
                    //TODO EXECUTE CREATE TABLE IN SPARK
                    commitStatement(sb.toString());
                    Var tVar = new Var(var.getVarName(), var.getVarValue(), Var.DataType.TABLE);
                    tVar.setAliasName(aliasName);
                    addTableVars(tVar);
                    break;
                case EXPRESSION:
                    //1. 计算表达式的值 2. 将变量加入到容器
                    TreeNode statement = var.getExpr();
                    statement.setExecSession(getExecSession());
                    if (null == statement) {
                        break;
                    }
                    statement.execute();
                    var.setVarValue(((Var) statement.getRs().getObject(1)).getVarValue());
                    var.setExecuted(true);
                    break;
                default:
                    break;
            }
            addVar(var);
        }


        return 0;
    }

    @Override
    public BaseStatement createStatement() {
        return this;
    }
}
