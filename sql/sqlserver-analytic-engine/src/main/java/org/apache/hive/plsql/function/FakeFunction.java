package org.apache.hive.plsql.function;

import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 4/12 0012.
 */
public class FakeFunction extends BaseStatement {
    // store all function args
    List<Var> vars = new ArrayList<>();

    public void setVars(List<Var> args) {
        vars = args;
    }

    @Override
    public int execute() throws Exception {
        StringBuilder sb = new StringBuilder();
        for (Var var: vars) {
            if (var.getDataType() == Var.DataType.VAR) {
                Var paraVar = getExecSession().getVariableContainer().findVar(var.getVarName());
                if (paraVar != null)
                    sb.append(paraVar.toString()).append(" ");
                continue;
            }
            if (var.getValueType() == Var.ValueType.EXPRESSION) {
                TreeNode baseStatement = var.getExpr();
                baseStatement.setExecSession(getExecSession());
                baseStatement.execute();
                Var baseVar = (Var) baseStatement.getRs().getObject(0);
                sb.append(baseVar.toString()).append(" ");
                continue;
            }
            sb.append(var).append(" ");
        }
        System.out.println("function call arg: " + sb.toString());
        return 0;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public String doCodegen(List<String> imports, List<String> variables, List<Var> knownVars){
        StringBuffer sb = new StringBuffer();
        StringBuilder result = new StringBuilder();
        for (Var var: vars) {
            if (var.getDataType() == Var.DataType.VAR) {
                Object value = null;
                try{
                    value = var.getVarValue();
                } catch (ParseException e) {
                    //TODO
                }
                if(value != null){
                    result.append("\"" + value.toString() + "\"");
                } else {
                    result.append(var.getVarName());
                }
                result.append("+\" \"");
                continue;
            }
            if (var.getValueType() == Var.ValueType.EXPRESSION) {
                TreeNode baseStatement = var.getExpr();
                if(baseStatement instanceof BaseStatement){
                    String code = ((BaseStatement) baseStatement).doCodegen(imports, variables, knownVars);
                    result.append(code);
                    result.append("+\" \"");
                }
                continue;
            }
            result.append("\"" + var.toString() + "\"");
            result.append("+\" \"");
        }
        sb.append("System.out.println(\"function call arg: \" + " + result.toString() + " )");
        return sb.toString();
    }
}
