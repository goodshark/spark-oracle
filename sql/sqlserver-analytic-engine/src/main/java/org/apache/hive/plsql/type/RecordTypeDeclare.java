package org.apache.hive.plsql.type;

import org.apache.hive.tsql.arg.Var;

/**
 * Created by dengrb1 on 8/1 0001.
 */
public class RecordTypeDeclare extends LocalTypeDeclare {
    public RecordTypeDeclare() {
        super(Var.DataType.COMPOSITE);
    }

    private void checkTypeResolved() {
        boolean checkFlag = true;
        for (String varName: typeVarMap.keySet()) {
            Var typeVar = typeVarMap.get(varName);
            if (typeVar.getDataType() == Var.DataType.REF_COMPOSITE && !typeVar.isCompoundResolved())
                checkFlag = false;
        }
        if (checkFlag)
            setResolved();
    }

    @Override
    public int execute() throws Exception {
        for (String varName: typeVarMap.keySet()) {
            Var typeVar = typeVarMap.get(varName);
            if (typeVar.getDataType() == Var.DataType.REF_SINGLE)
                resolveRefSingle(typeVar);
            if (typeVar.getDataType() == Var.DataType.REF_COMPOSITE)
                resolveRefComposite(typeVar);
            if (typeVar.getDataType() == Var.DataType.CUSTOM)
                resolveCustomType(typeVar);
        }
        checkTypeResolved();
        addType(this);
        return 0;
    }
}
