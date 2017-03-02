package hive.tsql.common;

/**
 * Created by zhongdg1 on 2016/12/21.
 */
public enum AssignmentOp {
    EQ("="), ADD_EQ("+="), SUB_EQ("-="), MUL_EQ("*="), DIV_EQ("/="), MOD_EQ("%="), AND_EQ("&="), NOT_EQ("^="), OR_EQ("|=");

    public String val;

    AssignmentOp(String val) {
        this.val = val;
    }

    public static AssignmentOp createAop(String val) {
        for(AssignmentOp aop :AssignmentOp.values()) {
            if(val.equalsIgnoreCase(aop.val)) {
                return aop;
            }
        }
        return null;
    }


}
