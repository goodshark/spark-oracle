package org.apache.hive.tsql.cfl;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.ddl.CreateFunctionStatement;
import org.apache.hive.tsql.node.LogicNode;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.node.PredicateNode;

import java.text.ParseException;
import java.util.*;

/**
 * Created by dengrb1 on 12/7 0007.
 */
public class WhileStatement extends BaseStatement {

    private LogicNode condtionNode = null;
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

    public void setCondtionNode(LogicNode node) {
        condtionNode = node;
    }

    public LogicNode getCondtionNode() {
        return condtionNode;
    }

    public boolean isTrue() throws Exception {
        if (condtionNode != null) {
            condtionNode.setExecSession(getExecSession());
            condtionNode.execute();
            return condtionNode.getBool();
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
    public String doCodegen(){
        StringBuffer sb = new StringBuffer();
        if(this.condtionNode.getBool()){
            sb.append("while(true){");
            sb.append(CODE_LINE_END);
        } else {
            String needAppend = null;
            boolean reverse = false;
            if(this.condtionNode.getIndexIter() != null){
                Var loopvar = this.condtionNode.getIndexIter().getIndexVar();
                reverse = this.condtionNode.getIndexIter().isReverse();
                CreateFunctionStatement.SupportDataTypes type = CreateFunctionStatement.fromString(loopvar.getDataType().name());
                if(type != null){
                    sb.append(type.toString());
                    sb.append(CODE_SEP);
                    sb.append(loopvar.getVarName());
                    sb.append(CODE_EQ);
                    try{
                        if(!reverse){
                            sb.append(this.condtionNode.getIndexIter().getLower().getVarValue().toString());
                        } else {
                            sb.append(this.condtionNode.getIndexIter().getUpper().getVarValue().toString());
                        }
                    } catch (ParseException e){
                        //TODO
                    }
                    sb.append(CODE_END);
                    sb.append(CODE_LINE_END);
                    needAppend = loopvar.getVarName()+"++" + CODE_END;
                }
            }
            sb.append("while(");
            if(reverse && condtionNode.getChildrenNodes().size() == 2){
                LogicNode andNode = new LogicNode(TreeNode.Type.AND);
                LogicNode leftNotNode = new LogicNode(TreeNode.Type.AND);
                LogicNode rightNotNode = new LogicNode(TreeNode.Type.AND);
                andNode.addNode(leftNotNode);
                andNode.addNode(rightNotNode);
                PredicateNode leftPredicateNode = new PredicateNode(TreeNode.Type.PREDICATE);
                leftPredicateNode.setEvalType(PredicateNode.CompType.COMP);
                leftNotNode.addNode(leftPredicateNode);
                PredicateNode rightPredicateNode = new PredicateNode(TreeNode.Type.PREDICATE);
                rightPredicateNode.setEvalType(PredicateNode.CompType.COMP);
                rightNotNode.addNode(rightPredicateNode);
                leftPredicateNode.setOp(">=");
                rightPredicateNode.setOp("<=");
                TreeNode left = condtionNode.getChildrenNodes().get(0);
                for (TreeNode node : left.getChildrenNodes()){
                    rightPredicateNode.addNode(node);
                }
                TreeNode rift = condtionNode.getChildrenNodes().get(1);
                for (TreeNode node : left.getChildrenNodes()){
                    leftPredicateNode.addNode(node);
                }
                sb.append(andNode.doCodegen());
            } else {
                sb.append(condtionNode.doCodegen());
            }
            sb.append("){");
            sb.append(CODE_LINE_END);
            if(needAppend != null){
                sb.append(needAppend);
                sb.append(CODE_LINE_END);
            }
        }
        List<TreeNode> childs = this.getChildrenNodes();
        for(TreeNode child : childs){
            if(child instanceof BaseStatement){
                sb.append(((BaseStatement) child).doCodegen());
                sb.append(CODE_LINE_END);
            }
        }
        sb.append("}");
        sb.append(CODE_LINE_END);
        return sb.toString();
    }
}
