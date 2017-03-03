package org.apache.hive.tsql.arg;

import org.apache.hive.tsql.common.ExpressionComputer;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.util.DateUtil;
import org.apache.hive.tsql.util.StrUtils;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;

/**
 * Created by zhongdg1 on 2016/12/1.
 */
public class Var implements Serializable {


    private static final long serialVersionUID = -1631515791432293303L;

    public enum DataType {
        STRING, VARCHAR, LONG, DOUBLE, FLOAT, INT, INTEGER, DATE, BINARY, BIT, TABLE, CURSOR, NULL, VAR, DEFAULT, BOOLEAN, COMMON, FUNCTION
    }

    public enum ValueType {
        EXPRESSION, TABLE, CURSOR, SPECIAL, NONE, DEFAULT
    }

    public enum VarType {
        INPUT, OUTPUT
    }

    private String varName = null;
    private Object varValue;
    private DataType dataType = DataType.COMMON;
    private String aliasName;
    private VarType varType = VarType.INPUT;
    private ValueType valueType = ValueType.NONE;
    private TreeNode expr;
    private boolean isReadonly = false;
    private boolean isExecuted = false;
    private boolean isDefault = false;


    public Var(String varName, Object varValue, DataType dataType) {
        this.varName = null != varName ? varName.toUpperCase() : null;
        this.varValue = varValue;
        this.dataType = dataType;
    }

    public Var(String varName, TreeNode expr, DataType dataType) {
        this.varName = null != varName ? varName.toUpperCase() : null;
        this.expr = expr;
        this.dataType = dataType;
    }

    public Var(String varName, TreeNode expr) {
        this.varName = null != varName ? varName.toUpperCase() : null;
        this.expr = expr;
    }

    public Var(String varName, TreeNode expr, DataType dataType, VarType varType) {
        this.varName = null != varName ? varName.toUpperCase() : null;
        this.expr = expr;
        this.dataType = dataType;
        this.varType = varType;
    }


    public Var(Object varValue, DataType dataType) {
        this.varValue = varValue;
        this.dataType = dataType;
    }


    public Var(String varName) {
        this.varName = null != varName ? varName.toUpperCase() : null;
    }

    public Var() {
    }


    public Var clone() {
        Var v = new Var(this.varName, this.varValue, this.dataType);
        v.setValueType(this.valueType);
        v.setAliasName(this.aliasName);
        v.setVarType(this.varType);
        v.setExecuted(this.isExecuted);
        return v;
    }

    public static Var Null = new Var(DataType.NULL);


    public static Var TrueVal = new Var(true, DataType.BOOLEAN);

    public static Var FalseVal = new Var(false, DataType.BOOLEAN);

    public Var(DataType type) {
        this.dataType = type;
    }


    public TreeNode getExpr() {
        return expr;
    }

    public ValueType getValueType() {
        return valueType;
    }

    public void setExpr(TreeNode expr) {
        this.expr = expr;
    }

    public boolean isExecuted() {
        return isExecuted;
    }

    public void setExecuted(boolean executed) {
        isExecuted = executed;
    }

    public void setValueType(ValueType valueType) {
        this.valueType = valueType;
    }

    /**
     * 异或操作
     *
     * @param v
     * @return
     */
    public Var operatorXor(Var v) throws ParseException {
        ExpressionComputer expressionComputer = new ExpressionComputer();
        return expressionComputer.operatorXor(this, v);
    }

    /**
     * and操作
     *
     * @param v
     * @return
     */
    public Var operatorAnd(Var v) throws ParseException {
        ExpressionComputer expressionComputer = new ExpressionComputer();
        return expressionComputer.operatorAnd(this, v);
    }

    /**
     * or操作
     *
     * @param v
     * @return
     */
    public Var operatorOr(Var v) throws ParseException {
        ExpressionComputer expressionComputer = new ExpressionComputer();
        return expressionComputer.operatorOr(this, v);
    }


    /**
     * 求余
     *
     * @param v
     * @return
     */
    public Var operatorMod(Var v) throws ParseException {
        ExpressionComputer expressionComputer = new ExpressionComputer();
        return expressionComputer.operatorMod(this, v);
    }

    /**
     * Division operator
     */
    public Var operatorDiv(Var v) throws ParseException {
        ExpressionComputer expressionComputer = new ExpressionComputer();
        return expressionComputer.operatorDiv(this, v);
    }


    /**
     * Multiplication operator
     */
    public Var operatorMultiply(Var v) throws ParseException {
        ExpressionComputer expressionComputer = new ExpressionComputer();
        return expressionComputer.operatorMultiply(this, v);
    }

    /**
     * Subtraction operator
     */
    public Var operatorSub(Var v) throws ParseException {
        ExpressionComputer expressionComputer = new ExpressionComputer();
        return expressionComputer.operatorSub(this, v);
    }


    /**
     * Addition operator
     */
    public Var operatorAdd(Var v) throws ParseException {
        ExpressionComputer expressionComputer = new ExpressionComputer();
        return expressionComputer.operatorAdd(this, v);
    }


    /**
     * Compare values
     */
    public int compareTo(Var v) throws ParseException {
        ExpressionComputer expressionComputer = new ExpressionComputer();
        return expressionComputer.compareTo(this, v);
    }

    /**
     * Compare values
     */
    @Override
    public boolean equals(Object obj) {
        if (getClass() != obj.getClass()) {
            return false;
        }
        Var var = (Var) obj;
        ExpressionComputer expressionComputer = new ExpressionComputer();
        boolean rs = false;
        try {
            rs = expressionComputer.equals(this, var);
        } catch (ParseException e) {
            System.out.println(e);
        }
        return rs;
    }


    public VarType getVarType() {
        return varType;
    }

    public void setVarType(VarType varType) {
        this.varType = varType;
    }

    public String getVarName() {
//        return varName.toUpperCase();
        return this.varName = null != varName ? varName.toUpperCase() : null;
    }

    public void setVarName(String varName) {
        this.varName = null != varName ? varName.toUpperCase() : null;
    }

    public Object getVarValue() throws ParseException {
        if (null == varValue) {
            return null;
        }
        switch (dataType) {
            case INT:
                varValue = Float.valueOf(varValue.toString()).intValue();
                break;
            case INTEGER:
                varValue = Float.valueOf(varValue.toString()).intValue();
                break;
            case FLOAT:
                varValue = Float.valueOf(varValue.toString());
                break;
            case STRING:
                varValue = String.valueOf(varValue.toString());
                break;
            case VARCHAR:
                varValue = String.valueOf(varValue.toString());
                break;
            case DATE:
                if (varValue instanceof String) {
                    varValue = DateUtil.parse(StrUtils.trimQuot(varValue.toString()));
                }

                break;
            default:
                break;
        }
        return varValue;
    }

    public void setVarValue(Object varValue) {
        this.varValue = varValue;

    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public String getAliasName() {
        return aliasName;
    }

    public void setAliasName(String aliasName) {
        this.aliasName = aliasName;
    }

    public boolean isReadonly() {
        return isReadonly;
    }

    public void setReadonly(boolean readonly) {
        isReadonly = readonly;
    }

    public int getInt() {
        return Integer.parseInt(varValue.toString());
    }

    public Date getDate() throws ParseException {
        return this.varValue instanceof Date ? (Date) this.varValue : DateUtil.parse(this.varValue.toString());
    }

    public Float getFloat() {
        return varValue instanceof Float ? (Float) varValue : Float.valueOf(varValue.toString());
    }

    public Double getDouble() {
        return varValue instanceof Double ? (Double) varValue : Double.valueOf(varValue.toString());
    }


    public boolean isDefault() {
        return isDefault;
    }

    public void setDefault(boolean aDefault) {
        isDefault = aDefault;
    }

    public String getString() {
        return varValue.toString();
    }

    @Override
    public String toString() {

        switch (dataType) {
            case DATE:
                try {
                    Date date = getDate();
                    return DateUtil.format(date, "yyyy-MM-dd");
                } catch (ParseException e) {
                    e.printStackTrace();
                }
                break;
            default:
                break;
        }
        return varValue == null ? null : varValue.toString();
    }
}


