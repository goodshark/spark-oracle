package org.apache.hive.tsql.arg;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.plsql.expression.MultiMemberExpr;
import org.apache.hive.tsql.common.ExpressionComputer;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dml.ExpressionStatement;
import org.apache.hive.tsql.util.DateUtil;
import org.apache.hive.tsql.util.StrUtils;
import org.apache.spark.sql.catalyst.plans.logical.Except;

import java.io.Serializable;
import java.text.ParseException;
import java.util.*;

/**
 * Created by zhongdg1 on 2016/12/1.
 */
public class Var implements Serializable {


    private static final long serialVersionUID = -1631515791432293303L;


    /**
     * REF_SINGLE:      a b%TYPE;
     * REF_COMPOSITE:   a b%ROWTYPE;
     * CUSTOM:          a b;
     * COMPOSITE:       TYPE RECORD is ...
     * VARRAY:          TYPE VARRAY(2) is ...
     * ASSOC_ARRAY:     TYPE TABLE is ... INDEX BY ...
     * NESTED_TABLE:    TYPE TABLE is ...
     */
    public enum DataType {
        STRING, VARCHAR, LONG, DOUBLE, FLOAT, INT, INTEGER, DATE, DATETIME, DATETIME2, TIME, LIST, TIMESTAMP,
        BINARY, BIT, TABLE, CURSOR, NULL, VAR, DEFAULT, BOOLEAN, COMMON, FUNCTION, BYTE, DECIMAL, EXCEPTION,
        SHORT, NESTED_TABLE, REF_SINGLE, REF_COMPOSITE, CUSTOM, COMPOSITE, VARRAY, ASSOC_ARRAY, NESTED_TABLE2
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


    // REF_SINGLE, REF_COMPOSITE
    private String refTypeName = "";
    // compound Type, like (int, string, string)
    private Map<String, Var> compoundVarMap = new HashMap<>();
    // compound Type mark
    private boolean compoundResolved = false;
    // array Type, like ((int, string), ...), first index 0 var is type only, real vars from 1 to size - 1
    private List<Var> arrayVars = new ArrayList<>();
    // only for general_element_part x(1).y
    private Var searchIndex = null;


    // TODO new custom type start
    // for SetStatement
    private MultiMemberExpr leftExpr = null;
    public void setLeftExpr(MultiMemberExpr expr) {
        leftExpr = expr;
    }
    public MultiMemberExpr getLeftExpr() {
        return leftExpr;
    }
    // var can be assigned value when initialized, for varray and nested-table
    private boolean initialized = false;
    public void setInitialized() {
        initialized = true;
    }

    private List<Var> varrayList = new ArrayList<>();

    public void addVarrayTypeVar(Var v) throws Exception {
        if (varrayList.size() != 0)
            throw new Exception("varray has already the type var");
        varrayList.add(0, v);
    }

    public void addVarrayValue(Var v) {
        varrayList.add(v);
    }

    public Var getVarrayInnerVar(int i) {
        return varrayList.get(i);
    }

    private List<Var> nestedTableList = new ArrayList<>();

    public void addNestedTableTypeVar(Var v) throws Exception {
        if (nestedTableList.size() != 0)
            throw new Exception("nested-table has already the type var");
        nestedTableList.add(0, v);
    }

    public Var getNestedTableTypeVar() throws Exception {
        if (nestedTableList.size() < 1)
            throw new Exception("nested-table has non type var");
        return nestedTableList.get(0);
    }

    public void addNestedTableValue(Var v) {
        nestedTableList.add(v);
    }

    public Var getNestedTableInnerVar(int i) throws Exception {
        if (nestedTableList.get(i) == null)
            throw new Exception("nested-table has no data");
        return nestedTableList.get(i);
    }

    private static class StringCmp implements Comparator<String> {
        @Override
        public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    }
    private TreeMap<String, Var> assocArray = new TreeMap<>(new StringCmp());
    // TYPE a is TABLE OF INTEGER INDEX BY VARCHAR(100), means VARCHAR(100)
    private Var assocTypeVar;
    // TYPE a is TABLE OF INTEGER INDEX BY VARCHAR(100), means INTEGER
    private Var assocValueTypeVar;

    public void setAssocTypeVar(Var v) {
        assocTypeVar = v;
    }

    public void setAssocValueTypeVar(Var v) {
        assocValueTypeVar = v;
    }

    public Var getAssocArrayValue(String index) {
        return assocArray.get(index);
    }

    public Var getAssocArrayValue(String index, Var val) {
        assocArray.put(index, assocValueTypeVar.clone());
        return assocArray.get(index);
    }

    public void addAssocArrayValue(String key, Var val) {
        assocArray.put(key, val);
    }

    // collection method
    private final static String collectionVarName = "COLLECTION-RESULT";

    public boolean isCollectionResult() {
        return varName.equals(collectionVarName);
    }

    public static boolean isCollectionMethod(String name) {
        MethodName[] methodNames = MethodName.values();
        for (MethodName methodName: methodNames) {
            if (methodName.name().equalsIgnoreCase(name))
                return true;
        }
        return false;
    }

    private enum MethodName {
        COUNT, DELETE, EXISTS, EXTEND, FIRST, LAST, LIMIT, NEXT, PRIOR, TRIM
    }
    public static Var callCollectionMethod(Var var, String method, Object...args) throws Exception {
        if (var.getDataType() != DataType.VARRAY && var.getDataType() != DataType.NESTED_TABLE
            && var.getDataType() != DataType.ASSOC_ARRAY)
            return null;
        Var resultVar = new Var();
        resultVar.setVarName(collectionVarName);
        switch (MethodName.valueOf(method.toUpperCase())) {
            case COUNT:
                int cnt = var.getCollectionCount();
                resultVar.setVarValue(cnt);
                resultVar.setDataType(DataType.INT);
                return resultVar;
            case DELETE:
                var.deleteCollection(args);
                return resultVar;
            case EXISTS:
                boolean exist = var.existsIndex(args);
                resultVar.setVarValue(exist);
                resultVar.setDataType(DataType.BOOLEAN);
                return resultVar;
            case EXTEND:
                var.extendSpace(args);
                return resultVar;
            case TRIM:
                var.trimSpace(args);
                return resultVar;
            case FIRST:
            case LAST:
                return var.getCollectionHeadTail(method.toUpperCase());
            case PRIOR:
            case NEXT:
                return var.traverseCollection(method.toUpperCase(), args);
            // do not exists default, just return null;
        }
        return null;
    }

    private int getCollectionCount() throws Exception {
        // do not count null placeholder
        switch (getDataType()) {
            case VARRAY:
                return varrayList.size() - 1;
            case NESTED_TABLE:
                // null placeholder
                int nestedCount = 0;
                for (int i = 1; i < nestedTableList.size(); i++) {
                    if (nestedTableList.get(i) != null)
                        nestedCount++;
                }
                return nestedCount;
            case ASSOC_ARRAY:
                // null placeholder
                int assocCount = 0;
                for (String key: assocArray.keySet()) {
                    if (assocArray.get(key) != null)
                        assocCount++;
                }
                return assocCount;
            default:
                throw new Exception("var " + getVarName() + " can not support count method");
        }
    }

    private void deleteCollection(Object ...args) throws Exception {
        switch (getDataType()) {
            case VARRAY:
                if (args.length > 0)
                    throw new Exception("varray type not support delete with arg");
                varrayList.clear();
                break;
            case NESTED_TABLE:
                if (args.length == 0)
                    nestedTableList = nestedTableList.subList(0,1);
                else if (args.length == 1) {
                    int index = (int) args[0];
                    if (index < nestedTableList.size()) {
                        nestedTableList.remove(index);
                        nestedTableList.add(index, null);
                    }
                } else {
                    int fromIndex = (int) args[0];
                    int toIndex = (int) args[1];
                    if (fromIndex > toIndex || fromIndex < 1 || toIndex >= nestedTableList.size())
                        return;
                    List<Var> tmp = new ArrayList<>();
                    tmp.addAll(nestedTableList.subList(0, fromIndex));
                    for (int i = fromIndex; i <= toIndex; i++)
                        tmp.add(null);
                    tmp.addAll(nestedTableList.subList(toIndex+1, nestedTableList.size()));
                    nestedTableList = tmp;
                }
                break;
            case ASSOC_ARRAY:
                if (args.length == 0)
                    assocArray = new TreeMap<>();
                else if (args.length == 1) {
                    Object key = args[0];
                    assocArray.remove(key);
                } else {
                    String fromKey = args[0].toString();
                    String toKey = args[1].toString();
                    try {
                        Map<String, Var> subMap = assocArray.subMap(fromKey, toKey);
                        assocArray = new TreeMap<>(subMap);
                    } catch (Exception e) {
                        // do nothing
                        return;
                    }
                }
                break;
            default:
                throw new Exception("var " + getVarName() + " can not support delete method");
        }
    }

    private boolean existsIndex(Object ...args) throws Exception {
        if (args.length != 1)
            throw new Exception("multi-member exists args error");
        switch (getDataType()) {
            case VARRAY:
                int vIndex = (int)args[0];
                if (vIndex < 1 || vIndex > varrayList.size())
                    return false;
                else
                    return true;
            case NESTED_TABLE:
                int nIndex = (int)args[0];
                if (nIndex < 1 || nIndex > nestedTableList.size())
                    return false;
                if (nestedTableList.get(nIndex) != null)
                    return true;
                else
                    return false;
            case ASSOC_ARRAY:
                break;
        }
        return false;
    }

    private void extendSpace(Object ...args) throws Exception {
        Var v = new Var(null, DataType.NULL);
        switch (getDataType()) {
            case VARRAY:
                throw new Exception("varray can not extend any more");
            case NESTED_TABLE:
                v.setDataType(getNestedTableTypeVar().getDataType());
                if (args.length == 0) {
                    nestedTableList.add(v);
                } else if (args.length == 1) {
                    int n = (int) args[0];
                    for (int i = 0; i < n; i++)
                        nestedTableList.add(v);
                } else {
                    int copys = (int) args[0];
                    int index = (int) args[1];
                    if (copys <= 0 || index < 1 || index > nestedTableList.size())
                        return;
                    for (int i = 0; i < copys; i++) {
                        Var copyVar = nestedTableList.get(index).clone();
                        nestedTableList.add(copyVar);
                    }
                }
                break;
            case ASSOC_ARRAY:
                throw new Exception("assoc-array can not extend any more");
        }
    }

    private void trimSpace(Object ...args) throws Exception {
        switch (getDataType()) {
            case VARRAY:
                if (args.length == 0) {
                    varrayList = varrayList.subList(0, varrayList.size()-1);
                } else {
                    int n = (int) args[0];
                    varrayList = varrayList.subList(0, varrayList.size()-n);
                }
                break;
            case NESTED_TABLE:
                break;
            case ASSOC_ARRAY:
                throw new Exception("assoc-array is not support trim");
        }
    }

    private Var getCollectionHeadTail(String action) throws Exception {
        Var resultVar = new Var();
        resultVar.setVarName(collectionVarName);
        switch (getDataType()) {
            case VARRAY:
                if (action.equalsIgnoreCase("FIRST")) {
                    getHeadUtil(varrayList, resultVar);
                } else {
                    getTailUtil(varrayList, resultVar);
                }
                break;
            case NESTED_TABLE:
                if (action.equalsIgnoreCase("FIRST")) {
                    getHeadUtil(nestedTableList, resultVar);
                } else {
                    getTailUtil(nestedTableList, resultVar);
                }
                break;
            case ASSOC_ARRAY:
                if (action.equalsIgnoreCase("FIRST")) {
                    for (String key: assocArray.keySet()) {
                        if (assocArray.get(key) != null) {
                            resultVar.setVarValue(key);
                            // TODO type is string
                            resultVar.setDataType(DataType.STRING);
                            break;
                        }
                    }
                } else {
                    for (String key: assocArray.descendingKeySet()) {
                        resultVar.setVarValue(key);
                        // TODO type is string
                        resultVar.setDataType(DataType.STRING);
                        break;
                    }
                }
                break;
        }
        return resultVar;
    }

    private void getHeadUtil(List<Var> list, Var v) {
        for (int i = 1; i < list.size(); i++) {
            if (list.get(i) != null) {
                v.setDataType(DataType.INT);
                v.setVarValue(i);
                return;
            }
        }
    }

    private void getTailUtil(List<Var> list, Var v) {
        for (int i = list.size()-1; i > 0; i--) {
            if (list.get(i) != null) {
                v.setDataType(DataType.INT);
                v.setVarValue(i);
                return;
            }
        }
    }

    private Var traverseCollection(String action, Object ...args) throws Exception {
        Var resultVar = new Var();
        resultVar.setVarName(collectionVarName);
        switch (getDataType()) {
            case VARRAY:
                if (action.equalsIgnoreCase("PRIOR")) {
                    getPrior(varrayList, resultVar, args);
                } else {
                    getNext(varrayList, resultVar, args);
                }
                break;
            case NESTED_TABLE:
                if (action.equalsIgnoreCase("PRIOR")) {
                    getPrior(nestedTableList, resultVar, args);
                } else {
                    getNext(nestedTableList, resultVar, args);
                }
                break;
            case ASSOC_ARRAY:
                String key = args[0].toString();
                if (action.equalsIgnoreCase("PRIOR")) {
                    boolean priorFlag = false;
                    for (String k: assocArray.descendingKeySet()) {
                        if (priorFlag) {
                            resultVar.setDataType(DataType.STRING);
                            resultVar.setVarValue(k);
                            break;
                        }
                        if (k.equalsIgnoreCase(key))
                            priorFlag = true;
                    }
                } else {
                    boolean nextFlag = false;
                    for (String k: assocArray.keySet()) {
                        if (nextFlag) {
                            resultVar.setDataType(DataType.STRING);
                            resultVar.setVarValue(k);
                            break;
                        }
                        if (k.equalsIgnoreCase(key))
                            nextFlag = true;
                    }
                }
                break;
        }
        return resultVar;
    }

    private void getPrior(List<Var> list, Var v, Object ...args) throws Exception {
        if (args.length == 0)
            return;
        int index = (int)Double.parseDouble(args[0].toString());
        for (int i = index-1; i > 0; i--) {
            if (list.get(i) != null) {
                v.setVarValue(i);
                v.setDataType(DataType.INT);
                return;
            }
        }
    }

    private void getNext(List<Var> list, Var v, Object ...args) throws Exception {
        if (args.length == 0)
            return;
        int index = (int)Double.parseDouble(args[0].toString());
        for (int i = index+1; i < list.size(); i++) {
            if (list.get(i) != null) {
                v.setVarValue(i);
                v.setDataType(DataType.INT);
                return;
            }
        }
    }
    // TODO new custom type end


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

    public static boolean isNull(Var v) throws Exception {
        switch ((v.getDataType())) {
            case COMPOSITE:
                // TODO may be need check value
                if (v.compoundVarMap.size() == 0)
                    return true;
                break;
            case VARRAY:
                if (v.varrayList.size() <= 1)
                    return true;
                break;
            case NESTED_TABLE:
                if (v.nestedTableList.size() <= 1)
                    return true;
                break;
            case ASSOC_ARRAY:
                if (v.assocArray.size() == 0)
                    return true;
                break;
            default:
                if (v.getVarValue() == null)
                    return true;
        }
        return false;
    }

    public static void assign(Var leftVar, Var rightVar) throws Exception {
        // maybe float <- int
        /*if (leftVar.getDataType() != rightVar.getDataType())
            throw new Exception("left var and right var is not same type");*/
        switch (leftVar.getDataType()) {
            case COMPOSITE:
                break;
            case VARRAY:
                leftVar.initialized = rightVar.initialized;
                leftVar.varrayList = new ArrayList<>(rightVar.varrayList);
                break;
            case NESTED_TABLE:
                leftVar.initialized = rightVar.initialized;
                leftVar.nestedTableList = new ArrayList<>(rightVar.nestedTableList);
                break;
            case ASSOC_ARRAY:
                leftVar.initialized = rightVar.initialized;
                leftVar.assocArray = new TreeMap<>(rightVar.assocArray);
                leftVar.assocValueTypeVar = rightVar.assocValueTypeVar.clone();
                leftVar.assocTypeVar = rightVar.assocTypeVar.clone();
                break;
            default:
                leftVar.setVarValue(rightVar.getVarValue());
        }
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
        if (dataType == DataType.REF_COMPOSITE) {
            for (String innerVarName: compoundVarMap.keySet()) {
                Var innerVar = compoundVarMap.get(innerVarName).typeClone();
                v.addInnerVar(innerVar);
            }
        }
        if (dataType == DataType.NESTED_TABLE) {
            // TODO compatible old implement
            for (Var arrayVar: arrayVars) {
                v.addArrayVar(arrayVar);
            }
            for (Var nestedTableInnerVar: nestedTableList) {
                v.addNestedTableValue(nestedTableInnerVar);
            }
        }
        // assoc-array
        if (assocTypeVar != null)
            v.assocTypeVar = assocTypeVar.clone();
        if (assocValueTypeVar != null)
            v.assocValueTypeVar = assocValueTypeVar.clone();
        return v;
    }

    public Var typeClone() {
        Var v = new Var(varName, null, dataType);
        v.setAliasName(aliasName);
        v.setVarType(varType);
        v.setExecuted(isExecuted);
        if (dataType == DataType.REF_COMPOSITE) {
            for (String innerVarName: compoundVarMap.keySet()) {
                Var innerVar = compoundVarMap.get(innerVarName).typeClone();
                v.addInnerVar(innerVar);
            }
        }
        if (dataType == DataType.NESTED_TABLE) {
            // TODO compatible old implement
            for (Var arrayVar: arrayVars) {
                v.addArrayVar(arrayVar);
            }
            for (Var nestedTableInnerVar: nestedTableList) {
                v.addNestedTableValue(nestedTableInnerVar);
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

    public Var operatorPower(Var v) throws Exception {
        ExpressionComputer expressionComputer = new ExpressionComputer();
        return expressionComputer.operatorPower(this, v);
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

    // composite || varray || table
}


