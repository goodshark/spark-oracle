package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseResultSet;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dml.ExpressionStatement;
import org.apache.hive.tsql.node.LogicNode;
import org.apache.spark.sql.catalyst.plfunc.PlFunctionRegistry;
import sun.reflect.generics.tree.Tree;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by chenfl2 on 2017/7/12.
 */
public class CaseStatement extends BaseStatement {

    private String switchVar = null;
    private boolean isSimple = true;
    private boolean isForValue;

    public CaseStatement(boolean isSimple, boolean isForValue) {
        super();
        this.isSimple = isSimple;
        this.isForValue = isForValue;
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

    public class CaseResultSet extends BaseResultSet {
        @Override
        public Object getObject(int columnIndex) throws SQLException {
            try {


                List<TreeNode> childrenNodes = getChildrenNodes();
                //紧跟case后面的表达式
                TreeNode caseInputNode = null;
                //else 表达式
                TreeNode elseNode = null;
                //swich表达式，包含when 和then表达式
                List<TreeNode> swichNode = new ArrayList<>();
                for (int i = 0; i < childrenNodes.size(); i++) {
                    TreeNode c = childrenNodes.get(i);
                    switch (c.getNodeType()) {
                        case CASE_INPUT:
                            caseInputNode = c;
                            break;
                        case WHEN:
                            swichNode.add(c);
                            break;
                        case ELSE:
                            elseNode = c;
                    }
                }

                if (isSimple) {
                    //简单表达式
                    caseInputNode.execute();
                    Var caseInputvar = (Var) caseInputNode.getRs().getObject(0);
                    for (TreeNode node : swichNode) {
                        List<TreeNode> swichchildrenNodes = node.getChildrenNodes();
                        if (swichchildrenNodes.size() != 1) {
                            return Var.Null;
                        }
                        TreeNode condition = ((CaseWhenPartStatement)node).getCondition();
                        condition.execute();
                        Var whenVar = (Var) condition.getRs().getObject(0);
                        if (caseInputvar.equals(whenVar)) {
                            swichchildrenNodes.get(0).execute();
                            return swichchildrenNodes.get(0).getRs().getObject(0);
                        }
                    }
                    elseNode.execute();
                    return elseNode.getRs().getObject(0);

                } else {
                    //搜索表达式
                    for (TreeNode node : swichNode) {
                        List<TreeNode> swichchildrenNodes = node.getChildrenNodes();
                        if (swichchildrenNodes.size() != 1) {
                            return Var.Null;
                        }
                        TreeNode condition = ((CaseWhenPartStatement)node).getCondition();
                        condition.execute();
                        boolean whenFlag = ((LogicNode) condition).getBool();
                        if (whenFlag) {
                            swichchildrenNodes.get(0).execute();
                            return swichchildrenNodes.get(0).getRs().getObject(0);
                        }
                    }
                    elseNode.execute();
                    return elseNode.getRs().getObject(0);
                }
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }
    }

    @Override
    public String getFinalSql() throws Exception {
        StringBuffer sql = new StringBuffer();
        sql.append("Case ");
        for(TreeNode child : getChildrenNodes()){
            sql.append(child.getFinalSql());
            sql.append(Common.SPACE);
        }
        sql.append("End ");
        return sql.toString();
    }

    @Override
    public String doCodegen(List<String> variables, List<String> childPlfuncs, PlFunctionRegistry.PlFunctionIdentify current, String returnType) throws Exception{
        StringBuffer sb = new StringBuffer();
        List<TreeNode> childs = getChildrenNodes();
        if(isSimple){
            for(TreeNode child : childs){
                if(Type.CASE_INPUT.equals(child.getNodeType()) && child instanceof BaseStatement){
                    switchVar = ((BaseStatement) child).doCodegen(variables, childPlfuncs, current, returnType);
                }
            }
        }
        if(isForValue) {
            String code = "";
            for(int i = childs.size() - 1; i >= 0; i-- ){
                TreeNode node = childs.get(i);
                if(Type.ELSE.equals(node.getNodeType())){
                    CaseElsePartStatement elseNode = ((CaseElsePartStatement)node);
                    if(elseNode.getChildrenNodes().size() == 1 && elseNode.getChildrenNodes().get(0) instanceof BaseStatement){
                        code = "(" + ((BaseStatement)elseNode.getChildrenNodes().get(0)).doCodegen(variables, childPlfuncs, current, returnType) + ")";
                    }
                } else if (Type.WHEN.equals(node.getNodeType())) {
                    CaseWhenPartStatement whenNode = ((CaseWhenPartStatement)node);
                    String str = ((BaseStatement)whenNode.getCondition()).doCodegen(variables, childPlfuncs, current, returnType);
                    String dataCode = "";
                    if(whenNode.getChildrenNodes().size() == 1 && whenNode.getChildrenNodes().get(0) instanceof BaseStatement){
                        dataCode = ((BaseStatement)whenNode.getChildrenNodes().get(0)).doCodegen(variables, childPlfuncs, current, returnType);
                    }
                    if(isSimple){
                        if(str != null && str.startsWith("\"") && str.endsWith("\"")){
                            code = "(" + str + ".equals(" + switchVar + ") ? " + dataCode  + ":" + code + ")";
                        } else {
                            code = "(" + str + CODE_EQ2 + switchVar + " ? " + dataCode  + ":" + code + ")";
                        }
                    } else {
                        code = "(" + str + "?" + dataCode + ":" + code + ")";
                    }
                }
            }
            sb.append(code);
        } else {
            for(TreeNode child : childs){
                if(child instanceof BaseStatement && (Type.WHEN.equals(child.getNodeType()) || Type.ELSE.equals(child.getNodeType()))){
                    if(child instanceof CaseWhenPartStatement && switchVar != null) {
                        ((CaseWhenPartStatement) child).setSwitchVar(switchVar);
                        sb.append(((CaseWhenPartStatement) child).doCodegen(variables, childPlfuncs, current, returnType));
                    } else {
                        sb.append(((BaseStatement)child).doCodegen(variables, childPlfuncs, current, returnType));
                    }
                }
            }
        }
        return sb.toString();
    }
}
