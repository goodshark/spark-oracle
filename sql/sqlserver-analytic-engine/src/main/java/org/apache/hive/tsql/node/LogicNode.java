package org.apache.hive.tsql.node;

import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.arg.VariableContainer;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dml.ExpressionStatement;
import org.apache.hive.tsql.udf.BaseCalculator;

import java.util.List;

/**
 * Created by dengrb1 on 12/5 0005.
 */
public class LogicNode extends ExpressionStatement {

    public static class IndexIterator {
        private Var indexVar = null;
        private Var lower = null;
        private Var upper = null;
        private Var curIndex = new Var();
        private boolean reverse = false;
        private boolean initExecute = true;

        public void setIndexVar(Var index) {
            indexVar = index;
        }

        public void setLower(Var left) {
            lower = left;
        }

        public Var getLower() {
            return lower;
        }

        public void setUpper(Var right) {
            upper = right;
        }

        public Var getUpper() {
            return upper;
        }

        public void setReverse() {
            reverse = true;
        }

        public boolean isReverse() {
            return reverse;
        }

        public void init() throws Exception {
            if (indexVar == null || lower == null || upper == null)
                return;

            if (reverse) {
                curIndex.setVarValue(upper.getVarValue());
                curIndex.setDataType(upper.getDataType());
            } else {
                curIndex.setVarValue(lower.getVarValue());
                curIndex.setDataType(lower.getDataType());
            }
        }

        public Var getIndexVar() {
            return indexVar;
        }

        private Var preExecute(VariableContainer variableContainer) throws Exception {
            if (initExecute) {
                if (lower.getDataType() == Var.DataType.VAR) {
                    Var lowerVar = variableContainer.findVar(lower.getVarName());
                    lower.setVarValue(lowerVar.getVarValue());
                    lower.setDataType(lowerVar.getDataType());
                }
                if (upper.getDataType() == Var.DataType.VAR) {
                    Var upperVar = variableContainer.findVar(upper.getVarName());
                    upper.setVarValue(upperVar.getVarValue());
                    upper.setDataType(upperVar.getDataType());
                }
                init();
                initExecute = false;
            }
            indexVar.setVarValue(curIndex.getVarValue());
            int nextIndex = (int)curIndex.getVarValue() + 1;
            curIndex.setVarValue(nextIndex);
            return indexVar;
        }
    }

    private boolean notFlag = false;
    private String logicStr = null;

    private boolean priority = false;

    private boolean boolFlag = false;

    private IndexIterator indexIter = null;

    public LogicNode() {
        super();
    }

    public LogicNode(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void setPriority() {
        priority = true;
    }

    public boolean isPriority() {
        return priority;
    }

    public void setNot() {
        notFlag = true;
    }

    public boolean getNot() {
        return notFlag;
    }

    public boolean getBool() {
        return boolFlag;
    }

    public void setBool(boolean bool) {
        boolFlag = bool;
    }

    public void setIndexIter(IndexIterator iter) {
        indexIter = iter;
    }

    public IndexIterator getIndexIter() {
        return indexIter;
    }

    public Var getIndexVar() {
        if (indexIter != null)
            return indexIter.getIndexVar();
        else
            return null;
    }

    public void preExecute() throws Exception {
        if (indexIter != null) {
            Var index = indexIter.preExecute(getExecSession().getVariableContainer());
            addVar(index);
        }
    }

    public void initIndex() throws Exception {
        if(indexIter != null)
            indexIter.init();
    }

    @Override
    public int execute() throws Exception {
        preExecute();
        List<TreeNode> list = getChildrenNodes();
        if (getNodeType() == Type.OR) {
            executeOr(list, true);
        } else if (getNodeType() == Type.AND) {
            executeAnd(list, true);
        } else if (getNodeType() == Type.NOT) {
            executeNot(list, true);
        }
        // compatible with ExpressionStatement
        setRs(new SparkResultSet().addRow(new Object[] {new Var("bool result", getBool(), Var.DataType.BOOLEAN)}));
        return 0;
    }

    private int executeOr(List<TreeNode> list, boolean exec) throws Exception {
        if (list.size() != 2)
            return -1;
        LogicNode left = (LogicNode) list.get(0);
        LogicNode right = (LogicNode) list.get(1);

        if (!exec) {
            logicStr = left.toString() + " or " + right.toString();
            return 0;
        }

        // short path
        left.execute();
        if (left.getBool()) {
            setBool(true);
        } else {
            right.execute();
            if (right.getBool())
                setBool(true);
            else
                setBool(false);
        }
        return 0;
        /*left.execute();
        right.execute();
        if (left.getBool() || right.getBool()) {
            setBool(true);
            return 0;
        } else {
            setBool(false);
            return 0;
        }*/
    }

    private int executeAnd(List<TreeNode> list, boolean exec) throws Exception {
        if (list.size() != 2)
            return -1;
        LogicNode left = (LogicNode) list.get(0);
        LogicNode right = (LogicNode) list.get(1);

        if (!exec) {
            logicStr = left.toString() + " and " + right.toString();
            return 0;
        }

        // short path
        left.execute();
        if (!left.getBool()) {
            setBool(false);
        } else {
            right.execute();
            if (right.getBool())
                setBool(true);
            else
                setBool(false);
        }
        return 0;
        /*left.execute();
        right.execute();
        if (left.getBool() && right.getBool()) {
            setBool(true);
            return 0;
        } else {
            setBool(false);
            return 0;
        }*/
    }

    private int executeNot(List<TreeNode> list, boolean exec)  throws Exception {
        if (list.size() != 1)
            return -1;
        LogicNode node = (LogicNode) list.get(0);
        String notStr = getNot() ? "NOT " : "";

        if (!exec) {
            logicStr = notStr + node.toString();
            return 0;
        }

        node.execute();
        if (getNot()) {
            setBool(!node.getBool());
            return 0;
        } else {
            setBool(node.getBool());
            return 0;
        }
    }

    public String toString() {
        try {
            List<TreeNode> list = getChildrenNodes();
            if (getNodeType() == Type.OR) {
                executeOr(list, false);
            } else if (getNodeType() == Type.AND) {
                executeAnd(list, false);
            } else if (getNodeType() == Type.NOT) {
                executeNot(list, false);
            }
            if (isPriority())
                logicStr = "(" + logicStr + ")";
            return logicStr;
        } catch (Exception e) {
            e.printStackTrace();
        }
        /*if (logicStr != null)
            return logicStr;*/
        return logicStr;
    }

    // compatible with ExpressionStatement
    @Override
    public String getSql() {
        return toString();
    }

    @Override
    public String getOriginalSql() {
        return toString();
    }

    @Override
    public String getFinalSql() throws Exception {
        return toString();
    }

    @Override
    public String doCodegen(){
        StringBuffer sb = new StringBuffer();
        String op = this.getNodeType().name();
        if(this.boolFlag){
            sb.append("(true)");
        } else {
            if("NOT".equalsIgnoreCase(op) && this.getChildrenNodes().size() == 1){
                TreeNode node = this.getChildrenNodes().get(0);
                if(node instanceof BaseStatement){
                    sb.append(CODE_NOT);
                    sb.append("(");
                    sb.append(((BaseStatement) node).doCodegen());
                    sb.append(")");
                }
            }
            if("OR".equalsIgnoreCase(op) && this.getChildrenNodes().size() == 2){
                TreeNode left = this.getChildrenNodes().get(0);
                TreeNode rift = this.getChildrenNodes().get(1);
                if(left instanceof BaseStatement && rift instanceof BaseStatement){
                    sb.append("(");
                    sb.append(((BaseStatement) left).doCodegen());
                    sb.append(CODE_OR);
                    sb.append(((BaseStatement) rift).doCodegen());
                    sb.append(")");
                }
            }
            if("AND".equalsIgnoreCase(op)){
                List<TreeNode> childs = this.getChildrenNodes();
                sb.append("(");
                int i = 0;
                for(TreeNode child : childs){
                    i++;
                    if(child instanceof BaseStatement){
                        sb.append(((BaseStatement) child).doCodegen());
                        if(i != childs.size()){
                            sb.append(CODE_AND);
                        }
                    }
                }
                sb.append(")");
            }
        }
        return sb.toString();
    }
}
