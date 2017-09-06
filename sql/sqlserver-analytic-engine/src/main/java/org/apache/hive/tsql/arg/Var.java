package org.apache.hive.tsql.arg;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.tsql.common.ExpressionComputer;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dml.ExpressionStatement;
import org.apache.hive.tsql.util.DateUtil;
import org.apache.hive.tsql.util.StrUtils;

import java.io.Serializable;
import java.text.ParseException;
import java.util.*;

/**
 * Created by zhongdg1 on 2016/12/1.
 */
public class Var implements Serializable {


    private static final long serialVersionUID = -1631515791432293303L;


    public enum DataType {
        STRING, VARCHAR, LONG, DOUBLE, FLOAT, INT, INTEGER, DATE, DATETIME, DATETIME2, TIME, LIST, TIMESTAMP,
        BINARY, BIT, TABLE, CURSOR, NULL, VAR, DEFAULT, BOOLEAN, COMMON, FUNCTION, BYTE, DECIMAL, EXCEPTION,
        SHORT, REF, COMPLEX, CUSTOM, ARRAY
    }

    public enum ValueType {
        EXPRESSION, TABLE, CURSOR, SPECIAL, NONE, DEFAULT
    }

    public enum VarType {
        INPUT, OUTPUT, INOUT
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
    private boolean noCopy = false;
    // oracle a => b, a is OUT
    private String mapOutName = null;


    // REF, COMPLEX
    private String refTypeName = "";
    // compound Type, like (int, string, string)
    private Map<String, Var> compoundVarMap = new HashMap<>();
    // compound Type mark
    private boolean compoundResolved = false;
    // array Type, like ((int, string), ...), first index 0 var is type only, real vars from 1 to size - 1
    private List<Var> arrayVars = new ArrayList<>();
    // only for general_element_part x(1).y
    private Var searchIndex = null;


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


    // TODO deep copy all inner vars
    public Var clone() {
        Var v = new Var(this.varName, this.varValue, this.dataType);
        v.setValueType(this.valueType);
        v.setAliasName(this.aliasName);
        v.setVarType(this.varType);
        v.setExecuted(this.isExecuted);
        v.setExpr(this.expr);
        v.refTypeName = refTypeName;
        v.compoundResolved = compoundResolved;
        if (dataType == DataType.COMPLEX) {
            for (String innerVarName: compoundVarMap.keySet()) {
                Var innerVar = compoundVarMap.get(innerVarName).typeClone();
                v.addInnerVar(innerVar);
            }
        }
        if (dataType == DataType.ARRAY) {
            for (Var arrayVar: arrayVars) {
                v.addArrayVar(arrayVar);
            }
        }
        return v;
    }

    public Var typeClone() {
        Var v = new Var(varName, null, dataType);
        v.setAliasName(aliasName);
        v.setVarType(varType);
        v.setExecuted(isExecuted);
        if (dataType == DataType.COMPLEX) {
            for (String innerVarName: compoundVarMap.keySet()) {
                Var innerVar = compoundVarMap.get(innerVarName).typeClone();
                v.addInnerVar(innerVar);
            }
        }
        if (dataType == DataType.ARRAY) {
            for (Var arrayVar: arrayVars) {
                v.addArrayVar(arrayVar);
            }
        }
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

    public void setNoCopy() {
        noCopy = true;
    }

    public boolean isNoCopy() {
        return noCopy;
    }

    public void setMapOutName(String name) {
        mapOutName = name;
    }

    public String getMapOutName() {
        return mapOutName;
    }

    public Var operatorConcat(Var v) throws Exception {
        ExpressionComputer expressionComputer = new ExpressionComputer();
        return expressionComputer.operatorConcat(this, v);
    }

    public Var operatorRemainder(Var v) throws Exception {
        ExpressionComputer expressionComputer = new ExpressionComputer();
        return expressionComputer.operatorRemainder(this, v);
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
        if (null == varValue || null == varValue.toString()) {
            return null;
        }
        switch (dataType) {
            case INT:
            case INTEGER:
                varValue = Double.valueOf(varValue.toString()).intValue();
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
                    varValue = getDate();
                }

                break;
            default:
                break;
        }
        return varValue;
    }



    public String getExecString() throws ParseException {
        if (null == varValue) {
            return null;
        }
        String val = varValue.toString();
        switch (dataType) {
            case INT:
            case INTEGER:
            case FLOAT:
            case DOUBLE:
            case LONG:
                return StrUtils.trimQuot(val.toString());
            case STRING:
            case VARCHAR:
                val = StrUtils.addQuot(val);
                break;
            case DATE:
            case DATETIME:
            case DATETIME2:
                if (varValue instanceof String) {
                    val = StrUtils.addQuot(val);
                } else {
                    val = StrUtils.addQuot(getDateStr());
                }
                break;
            default:
                val = StrUtils.addQuot(val);
                break;
        }
        return val;
    }

    public void setVarValue(Object varValue) {
        this.varValue = varValue;

    }

    public DataType getDataType() {
        return dataType;
    }

    public Var setDataType(DataType dataType) {
        this.dataType = dataType;
        return this;
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
        if (varValue == null || StringUtils.isBlank(varValue.toString())) {
            return null;
        }
        return this.varValue instanceof Date ?
                (Date) this.varValue : DateUtil.parseLenient(StrUtils.trimQuot(this.varValue.toString()), getPattern());
    }

    public long getTime() throws ParseException {
        return getDate().getTime();
    }

    public String getDateStr() throws ParseException {
        if (varValue == null || StringUtils.isBlank(varValue.toString())) {
            return null;
        }
        return DateUtil.format(getDate(), getPattern());

//        return this.varValue instanceof Date ?
//                (Date) this.varValue : DateUtil.parseLenient(StrUtils.trimQuot(this.varValue.toString()), getPattern());
    }


    private String getPattern() {
        String pattern = "yyyy-MM-dd HH:mm:ss";
        switch (dataType) {
            case DATE:
                pattern = "yyyy-MM-dd";
                break;
            case DATETIME:
            case DATETIME2:
//                varValue = fillDate(varValue.toString());
                pattern = "yyyy-MM-dd HH:mm:ss.SSS";
                break;
            case TIME:
//                varValue = fillDate(varValue.toString());
                pattern = "HH:mm:ss";
                break;
        }
        return pattern;

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

    public void setRefTypeName(String ref) {
        refTypeName = ref;
    }

    public String getRefTypeName() {
        return refTypeName;
    }

    public void addInnerVar(Var var) {
        compoundVarMap.put(var.getVarName(), var);
    }

    public Var getInnerVar(String name) {
        return compoundVarMap.get(name);
    }

    public void addArrayVar(Var var) {
        arrayVars.add(var);
    }

    public Var getArrayVar(int index) {
        return arrayVars.get(index);
    }

    public int getArraySize() {
        // the index 0 is the TYPE meaning
        return arrayVars.size() - 1;
    }

    public void setSearchIndex(Var i) {
        searchIndex = i;
    }

    public Var getSearchIndex() {
        return searchIndex;
    }

    public int getRecordSize() {
        return compoundVarMap.size();
    }

    public void setCompoundResolved() {
        compoundResolved = true;
    }

    public boolean isCompoundResolved() {
        return compoundResolved;
    }

    @Override
    public String toString() {
        try {
            switch (dataType) {
                case DATE:
                case DATETIME:
                case DATETIME2:
                case TIME:
                    return getDateStr();
                case LIST:
                    String res = "";
                    for (Object obj : (List<Object>) getVarValue()) {
                        res += obj.toString();
                    }
                    return res;
                default:
                    break;
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return varValue == null ? null : varValue.toString();
    }

    public String getSql() {
        if (varValue != null)
            return varValue.toString();
        if (expr != null) {
            return expr.getSql();
        }
        return "";
    }

    public String getOriginalSql() {
        if (StringUtils.isNotBlank(varName))
            return varName;
        if (varValue != null)
            return varValue.toString();
        if (expr != null)
            return ((ExpressionStatement) expr).getOriginalSql();
        return "";
    }

    public String getFinalSql() throws Exception {
        if (StringUtils.isNotBlank(varName)) {
            if (isExecuted && varValue != null)
                return varValue.toString();
            else if (expr != null) {
                return ((ExpressionStatement) expr).getFinalSql();
            } else {
                return varName;
            }
        } else if (varValue != null) {
            return varValue.toString();
        } else if (expr != null) {
            return ((ExpressionStatement) expr).getFinalSql();
        }
        return "";
    }

    public boolean isDate() {
        boolean flag = false;
        switch (dataType) {
            case DATE:
            case DATETIME:
            case DATETIME2:
            case TIME:
            case TIMESTAMP:
                flag = true;
                break;
            default:
                break;
        }
        return flag;
    }

    public boolean isNumber() {
        boolean flag = false;
        switch (dataType) {
            case LONG:
            case INT:
            case INTEGER:
            case DOUBLE:
            case FLOAT:
                flag = true;
                break;
            default:
                break;
        }
        return flag;
    }
}


