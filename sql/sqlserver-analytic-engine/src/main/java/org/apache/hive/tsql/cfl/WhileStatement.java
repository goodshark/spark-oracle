package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.ddl.CreateFunctionStatement;
import org.apache.hive.tsql.node.LogicNode;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.node.PredicateNode;

import java.sql.ResultSet;
import java.text.ParseException;
import java.util.*;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class WhileStatement extends BaseStatement {

    private TreeNode condtionNode = null;
    // store index variables in loop: for i in 1..10
//    private Map<String, Var> varMap = new HashMap<>();
    // store labels with while
//    private Set<String> labels = new HashSet<>();
    // check labels belong to while is searched
//    private boolean labelSearched = false;

    public WhileStatement() {
    }

    public WhileStatement(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void setCondtionNode(TreeNode node) {
        condtionNode = node;
    }

    public TreeNode getCondtionNode() {
        return condtionNode;
    }

    public boolean isTrue() throws Exception {
        if (condtionNode != null) {
            condtionNode.setExecSession(getExecSession());
            condtionNode.execute();
            Var res = (Var) condtionNode.getRs().getObject(0);
            return (boolean) res.getVarValue();
        } else {
            return false;
        }
    }

    public int execute() throws Exception {
        return 0;
    }

    /*public void setLabelSearched(Set<String> labs) {
        labelSearched = true;
        labels.addAll(labs);
    }

    public boolean isLabelSearched() {
        return labelSearched;
    }*/

    /*public boolean existLabel(String name) {
        return labels.contains(name);
    }*/

    /*public void searchAllLabels() {
        labelSearched = true;
        TreeNode pNode = getParentNode();
        // a normal CFL-node in AST tree always has a parent node
        if (pNode == null) {
            return;
        }
        List<TreeNode> childList = pNode.getChildrenNodes();
        // label always before whileStmt
        for (TreeNode child: childList) {
            if (child.equals(this))
                break;
            else {
                if (child.getNodeType() == TreeNode.Type.GOTO) {
                    GotoStatement gotoStatement = (GotoStatement) child;
                    if (!gotoStatement.getAction()) {
                        labels.add(gotoStatement.getLabel());
                    }
                }
            }
        }
    }*/

    /*public void setLoopIndexVar(Var index) {
        varMap.put(index.getVarName(), index);
    }*/

    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public String doCodegen(List<String> variables, List<String> childPlfuncs) throws Exception{
        StringBuffer sb = new StringBuffer();
        ResultSet rs = condtionNode.getRs();
        boolean bool = rs == null ? false : (boolean)((Var)rs.getObject(0)).getVarValue();
        if(bool){
            sb.append("while(true){");
            sb.append(CODE_LINE_END);
        } else {
            boolean reverse = false;
            if(condtionNode instanceof LogicNode && ((LogicNode)this.condtionNode).getIndexIter() != null){
                Var loopvar = ((LogicNode)this.condtionNode).getIndexIter().getIndexVar();
                reverse = ((LogicNode)this.condtionNode).getIndexIter().isReverse();
                CreateFunctionStatement.SupportDataTypes type = CreateFunctionStatement.fromString(loopvar.getDataType().name());
                if(type != null){
                    sb.append("for(");
                    sb.append(type.toString());
                    sb.append(CODE_SEP);
                    sb.append(loopvar.getVarName());
                    sb.append(CODE_EQ);
                    try{
                        if(!reverse){
                            if(((LogicNode)this.condtionNode).getIndexIter().getLower().getVarValue() != null){
                                sb.append(((LogicNode)this.condtionNode).getIndexIter().getLower().getVarValue().toString());
                            } else {
                                sb.append(((LogicNode)this.condtionNode).getIndexIter().getLower().getVarName());
                            }
                            sb.append(CODE_END);
                            sb.append(loopvar.getVarName());
                            sb.append("<=");
                            if(((LogicNode)this.condtionNode).getIndexIter().getUpper().getVarValue() != null) {
                                sb.append(((LogicNode)this.condtionNode).getIndexIter().getUpper().getVarValue().toString());
                            } else {
                                sb.append(((LogicNode)this.condtionNode).getIndexIter().getUpper().getVarName());
                            }
                            sb.append(CODE_END);
                            sb.append(loopvar.getVarName());
                            sb.append("++");
                        } else {
                            if(((LogicNode)this.condtionNode).getIndexIter().getUpper().getVarValue() != null) {
                                sb.append(((LogicNode)this.condtionNode).getIndexIter().getUpper().getVarValue().toString());
                            } else {
                                sb.append(((LogicNode)this.condtionNode).getIndexIter().getUpper().getVarName());
                            }
                            sb.append(CODE_END);
                            sb.append(loopvar.getVarName());
                            sb.append("<=");
                            if(((LogicNode)this.condtionNode).getIndexIter().getLower().getVarValue() != null){
                                sb.append(((LogicNode)this.condtionNode).getIndexIter().getLower().getVarValue().toString());
                            } else {
                                sb.append(((LogicNode)this.condtionNode).getIndexIter().getLower().getVarName());
                            }
                            sb.append(CODE_END);
                            sb.append(loopvar.getVarName());
                            sb.append("++");
                        }
                    } catch (ParseException e){
                        //TODO
                    }
                }
            } else {
                sb.append("while(");
                sb.append(((BaseStatement)this.condtionNode).doCodegen(variables, childPlfuncs));
            }
            sb.append("){");
            sb.append(CODE_LINE_END);
        }
        List<TreeNode> childs = this.getChildrenNodes();
        for(TreeNode child : childs){
            if(child instanceof BaseStatement){
                sb.append(((BaseStatement) child).doCodegen(variables, childPlfuncs));
                sb.append(CODE_LINE_END);
            }
        }
        sb.append("}");
        sb.append(CODE_LINE_END);
        return sb.toString();
    }
}
