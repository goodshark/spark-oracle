package org.apache.hive.plsql.type;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.arg.Var.DataType.*;

/**
 * Created by dengrb1 on 9/13 0013.
 */
public class VarrayTypeDeclare extends LocalTypeDeclare {
    private Var size;
    private Var typeVar;

    public VarrayTypeDeclare() {
        super(Var.DataType.VARRAY);
    }

    public void setSize(Var n) {
        size = n;
    }

    public int getSize() throws Exception {
        return (int)size.getVarValue();
    }

    public void setTypeVar(Var v) {
        typeVar = v;
    }

    public Var getTypeVar() {
        return typeVar;
    }

    public Var.DataType getVarrayValueType() {
        return typeVar.getDataType();
    }

    @Override
    public int execute() throws Exception {
        // resolve typeVar
        switch (typeVar.getDataType()) {
            case REF_SINGLE:
                resolveRefSingle(typeVar);
                break;
            case REF_COMPOSITE:
                resolveRefComposite(typeVar);
                break;
            case CUSTOM:
                resolveCustomType(typeVar);
                break;
        }
        addType(this);
        return 0;
    }
}
