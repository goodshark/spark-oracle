package org.apache.hive.tsql.another;

/**
 * Created by zhongdg1 on 2017/2/16.
 */
public class VarString {
    private String value;
    private boolean isVariable = false;

    public VarString(String value) {
        this.value = value;
    }

    public String getValue() {
        return value.toUpperCase();
    }

    public void setValue(String value) {
        this.value = value;
    }

    public boolean isVariable() {
        return isVariable;
    }

    public void setVariable(boolean variable) {
        isVariable = variable;
    }
}
