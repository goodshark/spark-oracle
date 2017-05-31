package org.apache.hive.tsql.common;

import org.apache.hive.tsql.arg.Var;

import java.text.ParseException;
import java.util.Set;

/**
 * Created by wangsm9 on 2017/2/7.
 */
public class ExpressionComputer {

    public boolean checkVarIsNumber(Var v1, Var v2) {
        Set<String> numberTypes = Number.getAllNuberType();
        if (numberTypes.contains(v1.getDataType().name()) && numberTypes.contains(v2.getDataType().name())) {
            return true;
        }
        return false;
    }

    public Number paserVarToNuber(Var var) throws ParseException {
        Number number = new Number();
        number.setValue(var.getVarValue().toString());
        String dataType = var.getDataType().name();
        number.setNumberType(Number.NumberType.valueOf(dataType));
        return number;
    }

    public Var paserNumberToVar(Number number) {
        Var var = new Var();
        var.setVarValue(number.getValue());
        String dataType = number.getNumberType().name();
        var.setDataType(Var.DataType.valueOf(dataType));
        return var;
    }

    /**
     *
     * @param v1
     * @param v2
     * @return v1 || v2
     * @throws Exception
     */
    public Var operatorConcat(Var v1, Var v2) throws Exception {
        StringBuilder sb = new StringBuilder();
        if (v1 != null)
            sb.append(v1.getVarValue().toString());
        if (v2 != null)
            sb.append(v2.getVarValue().toString());
        Var var = new Var();
        var.setDataType(Var.DataType.STRING);
        var.setVarValue(sb.toString());
        return var;
    }

    /**
     * and操作
     */
    public Var operatorXor(Var v1, Var v2) throws ParseException {
        if (v1.getVarValue() == null || v2.getVarValue() == null) {
            return Var.Null;
        } else if (v1.getDataType().equals(Var.DataType.INT) && v2.getDataType().equals(Var.DataType.INT)) {
            Var var = new Var();
            var.setDataType(Var.DataType.INT);
            int b1 = Integer.parseInt(v1.getVarValue().toString());
            int b2 = Integer.parseInt(v2.getVarValue().toString());
            var.setVarValue(b1 ^ b2);
            return var;
        }
        return Var.Null;
    }


    /**
     * and操作
     */
    public Var operatorAnd(Var v1, Var v2) throws ParseException {
        if (v1.getVarValue() == null || v2.getVarValue() == null) {
            return Var.FalseVal;
        } else if (v1.getDataType().equals(Var.DataType.BOOLEAN) && v2.getDataType().equals(Var.DataType.BOOLEAN)) {
            if (Boolean.parseBoolean(v1.getVarValue().toString()) && Boolean.parseBoolean(v1.getVarValue().toString()))
                return Var.TrueVal;
        } else if (v1.getDataType().equals(Var.DataType.INT) && v2.getDataType().equals(Var.DataType.INT)) {
            Var var = new Var();
            var.setDataType(Var.DataType.INT);
            int b1 = Integer.parseInt(v1.getVarValue().toString());
            int b2 = Integer.parseInt(v2.getVarValue().toString());
            var.setVarValue(b1 & b2);
            return var;
        }
        return Var.FalseVal;
    }

    /**
     * or操作
     */
    public Var operatorOr(Var v1, Var v2) throws ParseException {
        if (v1.getVarValue() == null || v2.getVarValue() == null) {
            return Var.FalseVal;
        } else if (v1.getDataType().equals(Var.DataType.BOOLEAN) && v2.getDataType().equals(Var.DataType.BOOLEAN)) {
            if (Boolean.parseBoolean(v1.getVarValue().toString()) || Boolean.parseBoolean(v1.getVarValue().toString()))
                return Var.TrueVal;
        } else if (v1.getDataType().equals(Var.DataType.INT) && v2.getDataType().equals(Var.DataType.INT)) {
            Var var = new Var();
            var.setDataType(Var.DataType.INT);
            int b1 = Integer.parseInt(v1.getVarValue().toString());
            int b2 = Integer.parseInt(v2.getVarValue().toString());
            var.setVarValue(b1 | b2);
            return var;
        }
        return Var.FalseVal;
    }


    /**
     * 求余
     */
    public Var operatorMod(Var v1, Var v2) throws ParseException {
        if (v1.getVarValue() == null || v2.getVarValue() == null) {
            return Var.Null;
        } else if (checkVarIsNumber(v1, v2)) {
            Number number = new Number();
            number = number.operator(paserVarToNuber(v1), paserVarToNuber(v2), Number.Operator.MOD);
            return paserNumberToVar(number);
        }
        return Var.Null;
    }

    /**
     * Division operator
     */
    public Var operatorDiv(Var v1, Var v2) throws ParseException {
        if (v1.getVarValue() == null || v2.getVarValue() == null) {
            return Var.Null;
        } else if (checkVarIsNumber(v1, v2)) {
            Number number = new Number();
            number = number.operator(paserVarToNuber(v1), paserVarToNuber(v2), Number.Operator.DIV);
            return paserNumberToVar(number);
        }
        return Var.Null;
    }


    /**
     * Multiplication operator
     */
    public Var operatorMultiply(Var v1, Var v2) throws ParseException {
        if (v1.getVarValue() == null || v2.getVarValue() == null) {
            return Var.Null;
        } else if (checkVarIsNumber(v1, v2)) {
            Number number = new Number();
            number = number.operator(paserVarToNuber(v1), paserVarToNuber(v2), Number.Operator.MULTIPLY);
            return paserNumberToVar(number);
        }
        return Var.Null;
    }

    /**
     * Subtraction operator
     */
    public Var operatorSub(Var v1, Var v2) throws ParseException {
        if (v1.getVarValue() == null || v2.getVarValue() == null) {
            return Var.Null;
        } else if (checkVarIsNumber(v1, v2)) {
            Number number = new Number();
            number = number.operator(paserVarToNuber(v1), paserVarToNuber(v2), Number.Operator.SUB);
            return paserNumberToVar(number);
        }
        return Var.Null;
    }


    /**
     * Addition operator
     */
    public Var operatorAdd(Var v1, Var v2) throws ParseException {
        if (v1.getVarValue() == null || v2.getVarValue() == null) {
            return Var.Null;
        } else if (v1.getDataType() == Var.DataType.STRING && v2.getDataType() == Var.DataType.STRING) {
            String values = v1.getVarValue().toString() + v2.getVarValue().toString();
            return new Var(values, Var.DataType.STRING);
        } else if (checkVarIsNumber(v1, v2)) {
            Number number = new Number();
            number = number.operator(paserVarToNuber(v1), paserVarToNuber(v2), Number.Operator.ADD);
            return paserNumberToVar(number);
        }
        return Var.Null;
    }


    /**
     * Compare values
     */
    public int compareTo(Var var1, Var var2) throws ParseException {
        if (equals(var1, var2)) {
            return 0;
        } else if (var1.getDataType().equals(Var.DataType.STRING) && var2.getDataType().equals(Var.DataType.STRING)) {
            return var1.getVarValue().toString().compareTo(var2.getVarValue().toString());
        } else if (var1.getDataType().equals(Var.DataType.DATE) || var2.getDataType().equals(Var.DataType.DATE)) {
//            return Long.compare(getDateTime(var1), getDateTime(var2));
            return Long.compare(var1.getTime(), var2.getTime());
        } else if (checkVarIsNumber(var1, var2)) {
            Number number = new Number();
            number = number.operator(paserVarToNuber(var1), paserVarToNuber(var2), Number.Operator.EQUALS);
            double v = Double.parseDouble(number.getValue());
            if (v == 0d) {
                return 0;
            } else if (v > 0d) {
                return 1;
            } else if (v < 0d) {
                return -1;
            }
        }
        return -1;
    }

//    private long getDateTime(Var var) throws ParseException {
//        Var.DataType dataType = var.getDataType();
//        long rs = -1l;
//        switch (dataType) {
//            case DATE:
//                rs = ((Date) var.getVarValue()).getTime();
//                break;
//            case STRING:
//                rs = DateUtil.parse(var.getVarValue().toString()).getTime();
//                break;
//            case INT:
//            case FLOAT:
//            case LONG:
//            case DOUBLE:
//                rs = Long.parseLong(var.getVarValue().toString());
//                break;
//        }
//        return rs;
//    }


    /**
     * Compare values
     */
    public boolean equals(Var var1, Var var2) throws ParseException {
        if (var1 == null && var2 == null) {
            return true;
        } else if (var1.getDataType().equals(Var.DataType.STRING) && var2.getDataType().equals(Var.DataType.STRING)) {
            if (var1.getVarValue().toString().equals(var2.getVarValue().toString())) {
                return true;
            }
        } else if (checkVarIsNumber(var1, var2)) {
            Number number = new Number();
            number = number.operator(paserVarToNuber(var1), paserVarToNuber(var2), Number.Operator.EQUALS);
            if (Double.parseDouble(number.getValue()) == 0d) {
                return true;
            }
        }
        return false;
    }

}
