package org.apache.hive.tsql.dml;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.*;
import org.apache.hive.tsql.ddl.CreateFunctionStatement;
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

    public ExpressionStatement() {}

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
                        case CONCAT:
                            var = leftChildrenVar.operatorConcat(rightChildrenVar);
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
        /*private Var variableComputer(Var var) throws Exception {
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
                TreeNode baseStatement = var.getExpr();
                if (baseStatement == null)
                    return var;
                baseStatement.setExecSession(getExecSession());
                baseStatement.execute();
                Var baseVar = (Var) baseStatement.getRs().getObject(0);
                var.setVarValue(baseVar.getVarValue());
            }
            return var;
        }*/


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
            TreeNode baseStatement = var.getExpr();
            if (baseStatement == null)
                return var;
            baseStatement.setExecSession(getExecSession());
            baseStatement.execute();
            Var baseVar = (Var) baseStatement.getRs().getObject(0);
            var.setVarValue(baseVar.getVarValue());
        }
        return var;
    }

    @Override
    public String getSql() {
        String sql = super.getSql();
        if (sql != null)
            return sql;
        List<TreeNode> childs = getChildrenNodes();
        OperatorSign sign = expressionBean.getOperatorSign();
        if (childs.size() == 0) {
            sql = expressionBean.getVar().getSql();
        } else if (childs.size() == 1) {
            ExpressionStatement es = (ExpressionStatement) childs.get(0);
            String varSql = es.getSql();
            if (sign == OperatorSign.BRACKET)
                sql = " ( " + varSql + " ) ";
            sql = sign.getOperator() + varSql;
        } else if (childs.size() == 2) {
            ExpressionStatement leftEs = (ExpressionStatement) childs.get(0);
            ExpressionStatement rightEs = (ExpressionStatement) childs.get(1);
            sql = leftEs.getSql() + " " + sign.getOperator() + " " + rightEs.getSql();
        }
        super.setSql(sql);
        return sql;
    }

    public String getOriginalSql() {
        String sql = super.getSql();
        if (sql != null)
            return sql;
        List<TreeNode> childs = getChildrenNodes();
        OperatorSign sign = expressionBean.getOperatorSign();
        if (childs.size() == 0) {
            sql = expressionBean.getVar().getOriginalSql();
        } else if (childs.size() == 1) {
            ExpressionStatement es = (ExpressionStatement) childs.get(0);
            String varSql = es.getOriginalSql();
            if (sign == OperatorSign.BRACKET)
                sql = " ( " + varSql + " ) ";
            sql = sign.getOperator() + varSql;
        } else if (childs.size() == 2) {
            ExpressionStatement leftEs = (ExpressionStatement) childs.get(0);
            ExpressionStatement rightEs = (ExpressionStatement) childs.get(1);
            sql = leftEs.getOriginalSql() + " " + sign.getOperator() + " " + rightEs.getOriginalSql();
        }
        super.setSql(sql);
        return sql;
    }

    @Override
    public String getFinalSql() throws Exception {
        String sql = "";
        List<TreeNode> childs = getChildrenNodes();
        OperatorSign sign = expressionBean.getOperatorSign();
        if (childs.size() == 0) {
            Var singleVar = expressionBean.getVar();
            if (singleVar.getDataType() == Var.DataType.VAR) {
                Var finalVar = variableComputer(singleVar);
                if (finalVar != null && finalVar.getDataType() != Var.DataType.NULL) {
                    sql = finalVar.getFinalSql();
                } else {
                    sql = singleVar.getFinalSql();
                }
            } else {
                sql = singleVar.getFinalSql();
            }
        } else if (childs.size() == 1) {
            ExpressionStatement es = (ExpressionStatement) childs.get(0);
            String varSql = es.getFinalSql();
            if (sign == OperatorSign.BRACKET)
                sql = " ( " + varSql + " ) ";
            sql = sign.getOperator() + varSql;
        } else if (childs.size() == 2) {
            ExpressionStatement leftEs = (ExpressionStatement) childs.get(0);
            ExpressionStatement rightEs = (ExpressionStatement) childs.get(1);
            sql = leftEs.getFinalSql() + " " + sign.getOperator() + " " + rightEs.getFinalSql();
        }
        return sql;
    }

    @Override
    public String doCodegen(List<String> variables, List<String> childPlfuncs) throws Exception{
        StringBuffer sb = new StringBuffer();
        Var var = getExpressionBean().getVar();
        OperatorSign op = getExpressionBean().getOperatorSign();
        if(op == null && var != null && getChildrenNodes().size() == 0){
            sb.append(BaseStatement.CODE_SEP);
            try{
                Object obj = var.getVarValue();
                if(obj != null){
                    CreateFunctionStatement.SupportDataTypes dataType = CreateFunctionStatement.fromString(var.getDataType().name());
                    if(CreateFunctionStatement.SupportDataTypes.STRING.equals(dataType) || CreateFunctionStatement.SupportDataTypes.CHAR.equals(dataType)){
                        String data = obj.toString();
                        if('\'' == data.charAt(0)){
                            data = data.substring(1, data.length() - 1);
                        }
                        if('\'' == data.charAt(data.length()-1)){
                            data = data.substring(0, data.length() - 2);
                        }
                        sb.append("\"");
                        sb.append(data);
                        sb.append("\"");
                    } else {
                        sb.append(obj.toString());
                    }
                } else {
                    sb.append(var.getVarName());
                }
            } catch (ParseException e) {
                //TODO
            }
            sb.append(BaseStatement.CODE_SEP);
        } else if(op == null && var != null && getChildrenNodes().size() == 1){
            if(getChildrenNodes().get(0) instanceof BaseStatement){
                BaseStatement bs = (BaseStatement)getChildrenNodes().get(0);
                sb.append(BaseStatement.CODE_SEP);
                sb.append(bs.doCodegen(variables, childPlfuncs));
                sb.append(BaseStatement.CODE_SEP);
            }
        } else if(op != null && var == null && getChildrenNodes().size() == 2){
            sb.append(CODE_SEP);
            if(getChildrenNodes().get(0) instanceof BaseStatement && getChildrenNodes().get(1) instanceof BaseStatement){
                BaseStatement bs0 = (BaseStatement)getChildrenNodes().get(0);
                sb.append(bs0.doCodegen(variables, childPlfuncs));
                sb.append(op.getOperator());
                BaseStatement bs1 = (BaseStatement)getChildrenNodes().get(1);
                sb.append(bs1.doCodegen(variables, childPlfuncs));
            }
            sb.append(CODE_SEP);
        }
        return sb.toString();
    }

}
