package org.apache.hive.tsql.cfl;

import org.apache.hive.basesql.cfl.NonSeqStatement;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.node.LogicNode;

import java.util.List;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class BreakStatement extends NonSeqStatement {

    public BreakStatement() {
    }

    public BreakStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public int execute() throws Exception {
        if (condition != null) {
            condition.setExecSession(getExecSession());
            condition.execute();
            Var res = (Var) condition.getRs().getObject(0);
            enable = (boolean) res.getVarValue();
        }
        return 0;
    }

    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public String doCodegen(List<String> variables, List<String> childPlfuncs) throws Exception{
        StringBuffer sb = new StringBuffer();
        if(condition != null){
            sb.append("if(");
            sb.append(((LogicNode)condition).doCodegen(variables, childPlfuncs));
            sb.append("){");
            sb.append(CODE_LINE_END);
            if(label != null){
                sb.append("break " + label);
            } else {
                sb.append("break ");
            }
            sb.append(CODE_END);
            sb.append(CODE_LINE_END);
            sb.append("}");
            sb.append(CODE_LINE_END);
        } else {
            if(label != null){
                sb.append("break " + label);
            } else {
                sb.append("break ");
            }
            sb.append(CODE_END);
            sb.append(CODE_LINE_END);
        }
        return sb.toString();
    }
}
