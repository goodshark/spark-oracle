package org.apache.hive.plsql.type;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.arg.Var.DataType.*;

/**
 * Created by dengrb1 on 9/13 0013.
 */
public class AssocArrayTypeDeclare extends LocalTypeDeclare {
    private Var indexTypeVar;

    public AssocArrayTypeDeclare() {
        super(Var.DataType.ASSOC_ARRAY);
    }

    public void setIndexTypeVar(Var v) {
        indexTypeVar = v;
    }

    public Var getIndexTypeVar() {
        return indexTypeVar;
    }

    @Override
    public int execute() throws Exception {
        // resolve tableTypeVar
        if (tableTypeVar.getDataType() == Var.DataType.REF_SINGLE)
            resolveRefSingle(tableTypeVar);
        if (tableTypeVar.getDataType() == Var.DataType.REF_COMPOSITE)
            resolveRefComposite(tableTypeVar);
        if (tableTypeVar.getDataType() == Var.DataType.CUSTOM)
            resolveCustomType(tableTypeVar);
        // resolve indexTypeVar
        switch (indexTypeVar.getDataType()) {
            case REF_SINGLE:
                resolveRefSingle(indexTypeVar);
                break;
            case REF_COMPOSITE:
                resolveRefComposite(indexTypeVar);
                break;
            case CUSTOM:
                resolveCustomType(indexTypeVar);
                break;
        }
        addType(this);
        return 0;
    }
}
