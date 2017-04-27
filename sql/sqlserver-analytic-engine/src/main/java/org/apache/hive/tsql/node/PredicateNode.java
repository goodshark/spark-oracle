package org.apache.hive.tsql.node;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.ColumnDataType;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.exception.CompareException;
import org.apache.hive.tsql.exception.WrongArgNumberException;
import org.apache.hive.tsql.util.StrUtils;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by dengrb1 on 12/5 0005.
 */
public class PredicateNode extends LogicNode {
    public enum CompType {EXISTS, COMP, COMPALL, COMPSOME, COMPANY, BETWEEN, IN, LIKE, IS}

    ;

    // private ArrayList<BaseStatement> exprList = new ArrayList<BaseStatement>();
    private List<TreeNode> exprList = new ArrayList<TreeNode>();
    private CompType type = null;

    private String origialOp = null;
    private String operator = null;

    private boolean notComp = false;

    private boolean boolFlag = false;

    private boolean compInQuery = false;

    private String predicateStr = null;

    public PredicateNode() {
        super();
    }

    public PredicateNode(TreeNode.Type t) {
        super();
        setNodeType(t);
    }

    public void setEvalType(CompType t) {
        type = t;
    }

    public void setOp(String op) {
        origialOp = op;
        transformOp();
    }

    public void setNotComp() {
        notComp = true;
    }

    private void transformOp() {
        if (origialOp.equalsIgnoreCase("=")) {
            operator = "==";
        } else if (origialOp.equalsIgnoreCase("<>")) {
            operator = "!=";
        } else if (origialOp.equalsIgnoreCase("!>")) {
//            setNotComp();
            operator = "<=";
        } else if (origialOp.equalsIgnoreCase("!<")) {
//            setNotComp();
            operator = ">=";
        } else {
            operator = origialOp;
        }
    }

    public void setExpr(BaseStatement expr) {
        exprList.add(expr);
    }

    public boolean getBool() {
        return boolFlag;
    }

    public void setBool(boolean bool) {
        boolFlag = bool;
    }

    // for Predicate compare expression IN subquery
    public void setCompInQuery() {
        compInQuery = true;
    }

    @Override
    public int execute() throws Exception {
        exprList = getChildrenNodes();
        boolean boolRes = false;
        if (type == CompType.EXISTS) {
            boolRes = compareExists(true);
        } else if (type == CompType.COMP) {
            boolRes = compare(true);
        } else if (type == CompType.COMPALL) {
            boolRes = compareAll(true);
        } else if (type == CompType.COMPSOME) {
            boolRes = compareSome(true);
        } else if (type == CompType.COMPANY) {
            boolRes = compareAny(true);
        } else if (type == CompType.BETWEEN) {
            boolRes = compareBetween(true);
        } else if (type == CompType.IN) {
            boolRes = compareIn(true);
        } else if (type == CompType.LIKE) {
            boolRes = compareLike(true);
        } else if (type == CompType.IS) {
            boolRes = compareIs(true);
        }
        setBool(boolRes);
        return 0;
    }

    private boolean checkVarNull(Var var) throws Exception {
        if (var == null || var.getVarValue() == null || var.getDataType() == Var.DataType.NULL)
            return true;
        return false;
    }

    private boolean compareExists(boolean exec) throws Exception {
        if (exprList.size() != 1)
            return false;
        BaseStatement expr = (BaseStatement) exprList.get(0);

        if (!exec) {
            predicateStr = "EXISTS (" + expr.getSql() + " ) ";
            return true;
        }

        expr.execute();
        ResultSet exprRes = expr.getRs();
        try {
            int rows = exprRes.getRow();
            if (rows == 0)
                return false;
            else
                return true;
        } catch (Exception e) {
            System.out.println("compare exists get exception: " + e.getMessage());
            e.printStackTrace();
        }
        return false;
    }

    private boolean compare(boolean exec) throws Exception {
        if (exprList.size() != 2)
            throw new WrongArgNumberException("compare");
//        BaseStatement leftExpr = (BaseStatement) exprList.get(0);
//        BaseStatement rightExpr = (BaseStatement) exprList.get(1);

        TreeNode leftExpr = exprList.get(0);
        TreeNode rightExpr = exprList.get(1);
        if (!exec) {
            predicateStr = leftExpr.getSql() + " " + origialOp + " " + rightExpr.getSql();
            return true;
        }
        leftExpr.execute();
        rightExpr.execute();
        ResultSet leftRes = leftExpr.getRs();
        ResultSet rightRes = rightExpr.getRs();
        try {
            Var leftVal = (Var) leftRes.getObject(0);
            Var rightVal = (Var) rightRes.getObject(0);
            if (checkVarNull(leftVal) || checkVarNull(rightVal))
                return false;
            // TODO test only
            //System.out.println("leftVal====>"+leftVal+",leftVal type is "+leftVal.getDataType().toString());
            // System.out.println("rightVal====>"+rightVal+",right type is "+rightVal.getDataType().toString());
            int compRes = leftVal.compareTo(rightVal);
            //System.out.println("compRes====>"+compRes);
            return getCompareResult(compRes);
        } catch (Exception e) {
            System.out.println("error");
            e.printStackTrace();
            throw new CompareException("compare: " + e.getMessage());
        }
    }

    // TODO this is temp, col type will be only one
    private Var.DataType transformColType(ColumnDataType t) {
        switch (t) {
            case STRING:
                return Var.DataType.STRING;
            case LONG:
                return Var.DataType.LONG;
            case INT:
                return Var.DataType.INT;
            case DOUBLE:
                return Var.DataType.FLOAT;
            case DATE:
                return Var.DataType.DATE;
            default:
                return Var.DataType.STRING;
        }
    }

    private boolean compareExpQuery(Var expression, SparkResultSet resSet, String comp) throws RuntimeException {
        try {
            List<ColumnDataType> typeList = resSet.getFileType();
            if (typeList.size() != 1)
                throw new WrongArgNumberException("compare " + comp);
            ColumnDataType colType = typeList.get(0);
            while (resSet.next()) {
                Object valObj = resSet.fetchRow().getColumnVal(0);
                Var colVar = new Var(valObj, transformColType(colType));
                // TODO test only
                System.out.println(colVar);
                boolean boolRes = false;
                if (checkVarNull(expression) || checkVarNull(colVar))
                    boolRes = false;
                else {
                    int compRes = expression.compareTo(colVar);
                    boolRes = getCompareResult(compRes);
                }
                switch (comp) {
                    case "ALL":
                        if (!boolRes) return false;
                        break;
                    case "ANY":
                    case "SOME":
                        if (boolRes) return true;
                        break;
                    default:
                        break;
                }
            }
        } catch (Exception e) {
            System.out.println("compare expression query error");
            e.printStackTrace();
            throw new CompareException("compare " + comp + ": " + e.getMessage());
        }

        switch (comp) {
            case "ALL":
                return true;
            case "ANY":
            case "SOME":
                return false;
            default:
                return false;
        }
    }

    private boolean compareAll(boolean exec) throws Exception {
        if (exprList.size() != 2)
            throw new WrongArgNumberException("compare ALL");
        BaseStatement leftExpr = (BaseStatement) exprList.get(0);
        BaseStatement rightExpr = (BaseStatement) exprList.get(1);

        if (!exec) {
            predicateStr = leftExpr.getSql() + " " + origialOp + " ALL (" + rightExpr.getSql() + ")";
            return true;
        }

        leftExpr.execute();
        rightExpr.execute();
        ResultSet leftRes = leftExpr.getRs();
        SparkResultSet rightRes = (SparkResultSet) rightExpr.getRs();

        try {
            Var leftVal = (Var) leftRes.getObject(0);
            return compareExpQuery(leftVal, rightRes, "ALL");
        } catch (Exception e) {
            System.out.println("compare all error");
            e.printStackTrace();
            throw e;
        }
    }

    private boolean compareSome(boolean exec) throws Exception {
        if (exprList.size() != 2)
            return false;
        BaseStatement leftExpr = (BaseStatement) exprList.get(0);
        BaseStatement rightExpr = (BaseStatement) exprList.get(1);
        if (!exec) {
            predicateStr = leftExpr.getSql() + " " + origialOp + " SOME (" + rightExpr.getSql() + ")";
            return true;
        }

        leftExpr.execute();
        rightExpr.execute();
        ResultSet leftRes = leftExpr.getRs();
        SparkResultSet rightRes = (SparkResultSet) rightExpr.getRs();
        try {
            Var leftVal = (Var) leftRes.getObject(0);
            return compareExpQuery(leftVal, rightRes, "SOME");
        } catch (Exception e) {
            System.out.println("compare some error");
            e.printStackTrace();
            throw e;
        }
    }

    private boolean compareAny(boolean exec) throws Exception {
        if (exprList.size() != 2)
            return false;
        BaseStatement leftExpr = (BaseStatement) exprList.get(0);
        BaseStatement rightExpr = (BaseStatement) exprList.get(1);
        if (!exec) {
            predicateStr = leftExpr.getSql() + " " + origialOp + " ANY (" + rightExpr.getSql() + ")";
            return true;
        }

        leftExpr.execute();
        rightExpr.execute();
        ResultSet leftRes = leftExpr.getRs();
        SparkResultSet rightRes = (SparkResultSet) rightExpr.getRs();
        try {
            Var leftVal = (Var) leftRes.getObject(0);
            return compareExpQuery(leftVal, rightRes, "ANY");
        } catch (Exception e) {
            System.out.println("compare any error");
            e.printStackTrace();
            throw e;
        }
    }

    private boolean compareBetween(boolean exec) throws Exception {
        if (exprList.size() != 3)
            return false;
        TreeNode expr = exprList.get(0);
        TreeNode exprStart = exprList.get(1);
        TreeNode exprEnd = exprList.get(2);

        if (!exec) {
            String not = notComp ? " NOT" : "";
            predicateStr = expr.getSql() + not + " BETWEEN " + exprStart.getSql() + " AND " + exprEnd.getSql();
            return true;
        }

        expr.execute();
        exprStart.execute();
        exprEnd.execute();
        ResultSet exprRes = expr.getRs();
        ResultSet exprStartRes = exprStart.getRs();
        ResultSet exprEndRes = exprEnd.getRs();
        try {
            Var exprVar = (Var) exprRes.getObject(0);
            Var exprStartVar = (Var) exprStartRes.getObject(0);
            Var exprEndVar = (Var) exprEndRes.getObject(0);
            // if any expr itself is null, return false, even exists NOT
            if (checkVarNull(exprVar) || checkVarNull(exprStartVar) || checkVarNull(exprEndVar))
                return false;
            int cmp = exprStartVar.compareTo(exprEndVar);
            if (cmp > 0) {
                return notComp ? true : false;
            } else {
                int leftRes = exprVar.compareTo(exprStartVar);
                int rightRes = exprVar.compareTo(exprEndVar);
                if (leftRes < 0)
                    return notComp ? true : false;
                if (leftRes == 0 || rightRes <= 0)
                    return notComp ? false : true;
                return notComp ? true : false;
            }
        } catch (Exception e) {
            System.out.println("compare between error: " + e.getMessage());
            e.printStackTrace();
            throw new CompareException("compare between " + e.getMessage());
        }
    }

    private boolean compareIn(boolean exec) throws Exception {
        if (exprList.size() != 2)
            return false;
        TreeNode leftExpr =  exprList.get(0);
        TreeNode rightExpr = exprList.get(1);

        if (!exec) {
            String not = notComp ? " NOT" : "";
            predicateStr = leftExpr.getSql() + not + " IN " + "(" + rightExpr.getSql() + ")";
            return true;
        }

        leftExpr.execute();
        rightExpr.execute();
        // TODO rightExpr is subquery
        ResultSet leftRes = leftExpr.getRs();
        boolean matched = false;
        if (compInQuery) {
            // right: subquery
            SparkResultSet rightQuery = (SparkResultSet) rightExpr.getRs();
            List<ColumnDataType> typeList = rightQuery.getFileType();
            if (typeList.size() != 1)
                return false;
            try {
                while (rightQuery.next()) {
                    Object colObj = rightQuery.fetchRow().getColumnVal(0);
                    ColumnDataType colT = typeList.get(0);
                    Var colVar = new Var(colObj, transformColType(colT));
                    Var leftVal = (Var) leftRes.getObject(0);
                    if (checkVarNull(leftVal))
                        return false;
                    if (checkVarNull(colVar))
                        continue;
                    if (leftVal.compareTo(colVar) == 0)
                        if (notComp) {
                            matched = true;
                            break;
                        } else
                            return true;
                }
            } catch (Exception e) {
                System.out.println("comp IN query error");
                e.printStackTrace();
                throw new CompareException("compare in: " + e.getMessage());
            }
        } else {
            // right: expression list
            ResultSet rightRes = rightExpr.getRs();
            try {
                Var leftVal = (Var) leftRes.getObject(0);
                if (checkVarNull(leftVal))
                    return false;
//                List<Var> rightValList = (List<Var>) rightRes.getObject(0);
                Var rightResVar = (Var) rightRes.getObject(0);
                List<Var> rightValList = (List<Var>) rightResVar.getVarValue();
                /*for (Var rightVal : rightValList) {
                    int res = leftVal.compareTo(rightVal);
                    if (res == 0) {
                        if (notComp) {
                            matched = true;
                            break;
                        } else {
                            return true;
                        }
                    }
                }*/
                for (Var rightVal : rightValList) {
                    if (rightVal.getDataType() == Var.DataType.LIST) {
                        List<Var> realVarList = (List<Var>) rightVal.getVarValue();
                        for (Var realVar : realVarList) {
                            if (checkVarNull(realVar))
                                continue;
                            int res = leftVal.compareTo(realVar);
                            if (res == 0) {
                                if (notComp) {
                                    matched = true;
                                    break;
                                } else {
                                    return true;
                                }
                            }
                        }
                    } else {
                        if (checkVarNull(rightVal))
                            return false;
                        int res = leftVal.compareTo(rightVal);
                        if (res == 0) {
                            if (notComp) {
                                matched = true;
                                break;
                            } else {
                                return true;
                            }
                        }
                    }

                }
            } catch (Exception e) {
                System.out.println("error");
                e.printStackTrace();
                throw new CompareException("compare in: " + e.getMessage());
            }
        }
        return notComp ? (matched ? false : true) : false;
    }

    private boolean compareIs(boolean exec) throws Exception {
        if (exprList.size() != 1)
            return false;
        TreeNode expr = exprList.get(0);
        if (!exec) {
            String not = notComp ? " NOT" : "";
            predicateStr = expr.getSql() + " IS" + not + " NULL ";
            return true;
        }

        expr.execute();
        ResultSet exprRes = expr.getRs();
        try {
            Var val = (Var) exprRes.getObject(0);
            if (null == val || val.getVarValue() == null || val.getDataType() == Var.DataType.NULL)
                return notComp ? false : true;
            else
                return notComp ? true : false;
        } catch (Exception e) {
            System.out.println("compare is error");
            e.printStackTrace();
            throw new CompareException("compare is " + e.getMessage());
        }
    }

    private boolean compareLike(boolean exec) throws Exception {
        if (exprList.size() < 2)
            return false;
        TreeNode strExpr = exprList.get(0);
        TreeNode patternStrExpr = exprList.get(1);
        TreeNode escapeStrExpr = exprList.size() == 3 ? exprList.get(2) : null;

        if (!exec) {
            String not = notComp ? " NOT" : "";
            predicateStr = strExpr.getSql() + not + " LIKE " + patternStrExpr.getSql();
            if (escapeStrExpr != null)
                predicateStr += " ESCAPE " + escapeStrExpr.getSql();
            return true;
        }

        strExpr.execute();
        patternStrExpr.execute();
        if (escapeStrExpr != null)
            escapeStrExpr.execute();
        ResultSet strRs = strExpr.getRs();
        ResultSet patternStrRs = patternStrExpr.getRs();

        try {
            Var strVar = (Var) strRs.getObject(0);
            Var patternVar = (Var) patternStrRs.getObject(0);
            Var escapeVar = null;
            String escapeStr = null;
            if (escapeStrExpr != null) {
                ResultSet escapeStrRs = escapeStrExpr.getRs();
                escapeVar = (Var) escapeStrRs.getObject(0);
                if (escapeVar.getDataType() == Var.DataType.STRING)
                    escapeStr = (String) escapeVar.getVarValue();
            }
            if (strVar == null || patternVar == null || null == strVar.getVarValue() || null == patternVar.getVarValue()
                    || strVar.getDataType() != Var.DataType.STRING || patternVar.getDataType() != Var.DataType.STRING)
                return false;
            String str = (String) strVar.getVarValue();
            String patternStr = (String) patternVar.getVarValue();

            String realPattern = "";
            patternStr = StrUtils.trimQuot(patternStr);
            escapeStr = StrUtils.trimQuot(escapeStr);
            if (escapeStr == null)
                realPattern = transformPattern(patternStr, null);
            else {
                realPattern = transformPattern(patternStr, escapeStr.toCharArray()[0]);
            }
            String realStr = str.replace("'", "").trim();
            Pattern pattern = Pattern.compile(realPattern);
            Matcher matcher = pattern.matcher(realStr);
            if (matcher.matches()) {
                return notComp ? false : true;
            } else {
                return notComp ? true : false;
            }
        } catch (Exception e) {
            System.out.println("compare like error");
            e.printStackTrace();
            throw new CompareException("compare like " + e.getMessage());
        }
    }

    private String cutString(String str) {
        // str: "'xyz'" -> "xyx"
        if (str == null)
            return null;
        str = str.trim();
        return str.substring(1, str.length() - 1);
    }

    private boolean getCompareResult(int res) {
        if (res == 0) {
            if (operator.equals("==") || operator.equals(">=") || operator.equals("<="))
                return true;
        } else if (res <= -1) {
            if (operator.equals("<") || operator.equals("<=") || operator.equals("!="))
                return notComp ? false : true;
            if (notComp && (operator.equals(">") || operator.equals(">=")))
                return notComp ? true : false;
        } else if (res >= 1) {
            if (operator.equals(">") || operator.equals(">=") || operator.equals("!="))
                return notComp ? false : true;
            if (notComp && (operator.equals("<") || operator.equals("<=")))
                return notComp ? true : false;
        }
        return false;
    }

    public String transformPattern(String patStr, Character escapeChar) {
        int curState = 0;
        StringBuilder newPattern = new StringBuilder();
        char[] ca = patStr.toCharArray();
        for (int i = 0; i < ca.length; i++) {
            // escape
            if (escapeChar != null && ca[i] == escapeChar) {
                char esCh = ca[i], nextCh = ca[++i];
                if ((nextCh - 'a' >= 0 && nextCh - 'a' < 26) ||
                        (nextCh - 'A' >= 0 && nextCh - 'A' < 26))
                    newPattern.append(nextCh);
                else {
                    newPattern.append('\\');
                    newPattern.append(nextCh);
                }
                continue;
            }
            // normal & bracket
            switch (ca[i]) {
                case '%':
                    if (curState != 2) newPattern.append(".*");
                    else newPattern.append(ca[i]);
                    break;
                case '_':
                    if (curState != 2) newPattern.append(".{1}");
                    else newPattern.append(ca[i]);
                    break;
                case '\\':
                case '*':
                case '.':
                case '{':
                case '}':
                case '(':
                case ')':
                case '?':
                case '|':
                case '+':
                    newPattern.append('\\');
                    newPattern.append(ca[i]);
                    break;
                case '[':
                    if (curState != 2) {
                        newPattern.append(ca[i]);
                        curState = 2;
                    } else {
                        newPattern.append("\\[");
                    }
                    break;
                case ']':
                    newPattern.append(ca[i]);
                    curState = 0;
                    break;
                default:
                    newPattern.append(ca[i]);
                    break;
            }
        }
        // we need enforce add ']' into patternStr
        if (curState == 2)
            newPattern.append(']');
        return newPattern.toString();
    }

    @Override
    public String toString() {
        try {
            exprList = getChildrenNodes();
        /*if (predicateStr != null)
            return predicateStr;*/
            if (type == CompType.EXISTS) {
                compareExists(false);
            } else if (type == CompType.COMP) {
                compare(false);
            } else if (type == CompType.COMPALL) {
                compareAll(false);
            } else if (type == CompType.COMPSOME) {
                compareSome(false);
            } else if (type == CompType.COMPANY) {
                compareAny(false);
            } else if (type == CompType.BETWEEN) {
                compareBetween(false);
            } else if (type == CompType.IN) {
                compareIn(false);
            } else if (type == CompType.LIKE) {
                compareLike(false);
            } else if (type == CompType.IS) {
                compareIs(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return predicateStr;
    }
}
