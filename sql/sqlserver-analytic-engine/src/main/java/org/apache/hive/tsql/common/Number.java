package org.apache.hive.tsql.common;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by wangsm9 on 2017/2/7.
 */
public class Number {
    public enum NumberType {
        STRING(0), INT(1),INTEGER(2), LONG(3), FLOAT(4), DOUBLE(5);

        private int priorty=1;
        NumberType(int i) {
            this.priorty=i;
        }
    }

    public static Set getAllNuberType() {
        Set<String> set = new HashSet<>();
        for (NumberType s : NumberType.values()) {
            set.add(s.name());
        }
        return set;
    }

    public enum Operator {
        ADD, SUB, MULTIPLY, DIV, MOD,EQUALS, REMAINDER, POWER
    }

    private String value;
    private NumberType numberType;

    public Number operator(Number n1, Number n2, Operator operator) {
        Map<String, Integer> map = new HashMap<>();
        map.put(n1.numberType.name(), 1);
        if (map.containsKey(n2.numberType.name())) {
            map.put(n2.numberType.name(), 2);
        }else{
            map.put(n2.numberType.name(), 1);
        }
        Number rs = new Number();
        Double n1Value = Double.parseDouble(n1.getValue());
        Double n2Value = Double.parseDouble(n2.getValue());
        Double rsValue = computer(operator, n1Value, n2Value);

        NumberType rsNumberType=getNumberType(n1,n2);
        rs.setNumberType(rsNumberType);
        switch (rsNumberType){
            case INT:
            case INTEGER:
                rs.setValue(Integer.toString(rsValue.intValue()));
                break;
            case FLOAT:
            case DOUBLE:
                rs.setValue(rsValue.toString());
                break;
            case LONG:
                rs.setValue(Long.toString(rsValue.longValue()));
                break;
        }

        return rs;

    }



    private Double computer(Operator operator, Double n1Value, Double n2Value) {
        Double rsValue = 0d;
        switch (operator) {
            case ADD:
                rsValue = n1Value + n2Value;
                break;
            case DIV:
                if(0 == n2Value.doubleValue()) {
                    throw new ArithmeticException();
                }
                rsValue = n1Value.doubleValue() / n2Value.doubleValue();
                break;
            case MOD:
                rsValue = n1Value % n2Value;
                break;
            case MULTIPLY:
                rsValue = n1Value * n2Value;
                break;
            case SUB:
                rsValue = n1Value - n2Value;
                break;
            case EQUALS:
                rsValue = n1Value - n2Value;
                break;
            case REMAINDER:
                rsValue = n1Value - (n2Value * Math.round(n1Value/n2Value));
                break;
            case POWER:
                rsValue = Math.pow(n1Value, n2Value);
                break;
        }
        return rsValue;
    }

    public NumberType getNumberType(Number n1,Number n2){
        if(n1.getNumberType().priorty>=n2.getNumberType().priorty){
            return n1.getNumberType();
        }
        return  n2.getNumberType();
    }



    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public NumberType getNumberType() {
        return numberType;
    }

    public void setNumberType(NumberType numberType) {
        this.numberType = numberType;
    }

    public static void main(String[] args) {
        System.out.println(NumberType.DOUBLE.name());
        System.out.println(getAllNuberType());
    }


}
