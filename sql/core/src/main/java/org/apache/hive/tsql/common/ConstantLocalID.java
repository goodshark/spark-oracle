package org.apache.hive.tsql.common;

/**
 * Created by zhongdg1 on 2017/2/20.
 */
public class ConstantLocalID {
    private String val;
    private boolean isVariable = false;

    public String getVal() {
        return val;
    }

    public void setVal(String val) {
        this.val = val;
    }

    public boolean isVariable() {
        return isVariable;
    }

    public void setVariable(boolean variable) {
        isVariable = variable;
    }

    @Override
    public String toString() {
        return this.val;
    }
}
