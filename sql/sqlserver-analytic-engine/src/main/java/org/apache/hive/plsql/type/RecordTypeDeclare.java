package org.apache.hive.plsql.type;

import org.apache.hive.tsql.arg.Var;
import org.apache.spark.sql.catalog.Column;
import org.apache.spark.sql.Dataset;

import java.util.Map;

/**
 * Created by dengrb1 on 8/1 0001.
 */
public class RecordTypeDeclare extends LocalTypeDeclare {
    public RecordTypeDeclare() {
        super(LocalTypeDeclare.Type.RECORD);
    }

    private void checkTypeResolved() {
        boolean checkFlag = true;
        for (String varName: typeVarMap.keySet()) {
            Var typeVar = typeVarMap.get(varName);
            if (typeVar.getDataType() == Var.DataType.COMPLEX && !typeVar.isCompoundResolved())
                checkFlag = false;
        }
        if (checkFlag)
            setResolved();
    }

    @Override
    public int execute() throws Exception {
        for (String varName: typeVarMap.keySet()) {
            Var typeVar = typeVarMap.get(varName);
            if (typeVar.getDataType() == Var.DataType.REF)
                resolveRefVar(typeVar);
            if (typeVar.getDataType() == Var.DataType.COMPLEX)
                resolveComplexVar(typeVar);
            if (typeVar.getDataType() == Var.DataType.CUSTOM)
                resolveCustomType(typeVar);
        }
        checkTypeResolved();
        addType(this);
        return 0;
    }
}
