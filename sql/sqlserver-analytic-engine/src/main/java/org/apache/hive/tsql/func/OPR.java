package org.apache.hive.tsql.func;

/**
 * Created by zhongdg1 on 2017/3/20.
 */
public enum OPR {
    ADD("+"), SUB("-");

    private String val;
    OPR(String val) {
        this.val = val;
    }

    public static OPR createAop(String val) {
        for(OPR aop :OPR.values()) {
            if(val.equalsIgnoreCase(aop.val)) {
                return aop;
            }
        }
        return null;
    }
}