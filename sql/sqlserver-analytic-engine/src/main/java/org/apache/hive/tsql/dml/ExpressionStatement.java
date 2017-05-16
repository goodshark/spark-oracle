package org.apache.hive.tsql.dml;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.*;
import org.apache.hive.tsql.node.LogicNode;
import org.apache.hive.tsql.util.StrUtils;

import java.io.Serializable;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.List;


/**
 * Created by wangsm9 on 2016/12/7.
 */
public class ExpressionStatement extends SqlStatement implements Serializable {


    private static final long serialVersionUID = -7592904882343118467L;

    public ExpressionStatement(ExpressionBean expressionBean) {
        this.expressionBean = expressionBean;
    }

    private ExpressionBean expressionBean;

    @Override
    public int execute() throws Exception {
        setRs(new ExpressionRs());
        return 0;
    }


    public class ExpressionRs extends BaseResultSet {
        @Override
        public Object getObject(int columnIndex) throws SQLException {
            try {
                List<TreeNode> children = getChildrenNodes();
                if (null == children || children.size() == 0) {
                    Var var = expressionBean.getVar();
                    //删除表达式起始位置和末尾位置的单引号
                    if(var.getDataType().equals(Var.DataType.STRING)){
                        String v=var.getVarValue().toString();
                        var.setVarValue(StrUtils.trimQuot(v));
                    }
                    Var.DataType dataType = var.getDataType();
                    Var rs = new Var();
                    switch (dataType) {
                        case NULL:
                            rs = Var.Null;
                            break;
                        case DEFAULT:
                            //待实现
                            break;
                        case VAR:
                            rs = variableComputer(var);
                            break;
                        default:
                            rs = var;
                            break;
                    }
                    return rs;
                } else if (children.size() == 1) {
                    OperatorSign operatorSign = expressionBean.getOperatorSign();
                    TreeNode baseStatement = children.get(0);
                    // full bool expression
                    if (operatorSign == OperatorSign.COMPLEX_BOOL) {
                        baseStatement.execute();
                        boolean bool = ((LogicNode) baseStatement).getBool();
                        return new Var("bool result", bool, Var.DataType.BOOLEAN);
                    }
                    //baseStatement.setExecSession(getExecSession());
                    baseStatement.execute();
                    Var childrenVar = (Var) baseStatement.getRs().getObject(0);
                    Var var = new Var();
                    var.setDataType(childrenVar.getDataType());
                    switch (operatorSign) {
                        case ADD:
                            var.setVarValue(childrenVar.getVarValue());
                            break;
                        case SUBTRACT:
                            var.setVarValue(unaryComputer(childrenVar));
                            break;
                        case BIT_NOT:
                            //TODO 二进制取反
                            int a=Integer.parseInt(childrenVar.getVarValue().toString());
                            var.setVarValue(~a);
                            break;
                        case BRACKET:
                            // 括号表达式
                            var = childrenVar;
                    }
                    return var;
                } else {
                    Var var = new Var();
                    OperatorSign operatorSign = expressionBean.getOperatorSign();
                    TreeNode leftBaseStatement = children.get(0);
                    //leftBaseStatement.setExecSession(getExecSession());
                    leftBaseStatement.execute();
                    Var leftChildrenVar = (Var) leftBaseStatement.getRs().getObject(0);


                    TreeNode rightBaseStatement = children.get(1);
                    //rightBaseStatement.setExecSession(getExecSession());
                    rightBaseStatement.execute();

                    Var rightChildrenVar = (Var) rightBaseStatement.getRs().getObject(0);

                    //TODO 两个表达式的计算
                    switch (operatorSign) {
                        case ADD:
                            var = leftChildrenVar.operatorAdd(rightChildrenVar);
                            break;
                        case SUBTRACT:
                            var = leftChildrenVar.operatorSub(rightChildrenVar);
                            break;
                        case MULTIPLY:
                            var = leftChildrenVar.operatorMultiply(rightChildrenVar);
                            break;
                        case DIVIDE:
                            var = leftChildrenVar.operatorDiv(rightChildrenVar);
                            break;
                        case MOD:
                            var = leftChildrenVar.operatorMod(rightChildrenVar);
                            break;
                        case AND:
                            var = leftChildrenVar.operatorAnd(rightChildrenVar);
                            break;
                        case OR:
                            var = leftChildrenVar.operatorOr(rightChildrenVar);
                            break;
                        case EQUAL:
                            boolean b = leftChildrenVar.equals(rightChildrenVar);
                            var = b ? Var.TrueVal : Var.FalseVal;
                            break;
                        case NOT_EQUAL:
                        case NOT_EQUAL_ANOTHER:
                            boolean bl = leftChildrenVar.equals(rightChildrenVar);
                            var = bl ? Var.FalseVal : Var.TrueVal;
                            break;
                        case GREATER_THAN:
                            int v = leftChildrenVar.compareTo(rightChildrenVar);
                            var = v > 0 ? Var.FalseVal : Var.TrueVal;
                            break;
                        case GREATER_THAN_OR_EQUAL:
                        case NOT_LESS_THEN:
                            int c = leftChildrenVar.compareTo(rightChildrenVar);
                            var = c >= 0 ? Var.FalseVal : Var.TrueVal;
                            break;
                        case NOT_GREATER_THAN:
                        case LESS_THEN_OR_EQUAL:
                            int ngt = leftChildrenVar.compareTo(rightChildrenVar);
                            var = ngt <= 0 ? Var.FalseVal : Var.TrueVal;
                            break;
                        case LESS_THEN:
                            int lt = leftChildrenVar.compareTo(rightChildrenVar);
                            var = lt < 0 ? Var.FalseVal : Var.TrueVal;
                            break;
                        case XOR:
                            var = leftChildrenVar.operatorXor(rightChildrenVar);
                            break;

                    }
                    return var;
                }
            } catch (Exception e) {
                throw new SQLException(e);
            }
        }


        /**
         * 一元表达式计算
         *
         * @param var
         * @return
         */
        private Object unaryComputer(Var var) throws ParseException {
            Object o = new Object();
            Var.DataType dt = var.getDataType();
            switch (dt) {
                case INT:
                    o = (int) var.getVarValue() * (-1);
                    break;
                case FLOAT:
                    o = Double.parseDouble(var.getVarValue().toString()) * (-1);
                    break;
            }
            return o;
        }

        /**
         * 变量类型的计算，即变量值保存的是statement
         *
         * @param var
         * @return
         * @throws SQLException
         */
        private Var variableComputer(Var var) throws Exception {
            if (var == null || var.getDataType() != Var.DataType.VAR) {
                return Var.Null;
            }
            String varName = var.getVarName();

            //两个@表示系统变量，一个@表示普通变量
            if (varName.indexOf("@@") != -1) {
                var = findSystemVar(varName);
            } else {
                var = findVar(varName);
            }
            if (null == var) {
                return Var.Null;
            }
            //如果计算过，直接取值
            if (var.isExecuted()) {
                return var;
            }
            if (var.getValueType() == Var.ValueType.EXPRESSION) {
                BaseStatement baseStatement = (BaseStatement) var.getExpr();
                //baseStatement.setExecSession(getExecSession());
                baseStatement.execute();
                Var baseVar = (Var) baseStatement.getRs().getObject(0);
                var.setVarValue(baseVar.getVarValue());
            }
            return var;
        }


    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }

    public ExpressionBean getExpressionBean() {
        return expressionBean;
    }

    public void setExpressionBean(ExpressionBean expressionBean) {
        this.expressionBean = expressionBean;
    }

}
