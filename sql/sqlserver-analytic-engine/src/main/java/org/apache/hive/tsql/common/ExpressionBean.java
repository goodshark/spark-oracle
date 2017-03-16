package org.apache.hive.tsql.common;

import org.apache.hive.tsql.arg.Var;

import java.io.Serializable;

/**
 * Created by wangsm9 on 2016/12/5.
 */
public class ExpressionBean implements Serializable {


    private static final long serialVersionUID = 6047728701916291600L;
    private Var var;
    private OperatorSign operatorSign;

    public OperatorSign getOperatorSign() {
        return operatorSign;
    }

    public void setOperatorSign(OperatorSign operatorSign) {
        this.operatorSign = operatorSign;
    }

    public Var getVar() {
        return var;
    }

    public void setVar(Var var) {
        this.var = var;
    }

}
