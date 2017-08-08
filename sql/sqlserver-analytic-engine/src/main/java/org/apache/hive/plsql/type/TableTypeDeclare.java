package org.apache.hive.plsql.type;

import org.apache.hive.tsql.arg.Var;

/**
 * Created by dengrb1 on 8/1 0001.
 */
public class TableTypeDeclare extends LocalTypeDeclare {
    public TableTypeDeclare() {
        super(LocalTypeDeclare.Type.TABLE);
    }

    @Override
    public int execute() throws Exception {
        if (arrayVar.getDataType() == Var.DataType.REF)
            resolveRefVar(arrayVar);
        if (arrayVar.getDataType() == Var.DataType.COMPLEX)
            resolveComplexVar(arrayVar);
        if (arrayVar.getDataType() == Var.DataType.CUSTOM)
            resolveCustomType(arrayVar);
        addType(this);
        return 0;
    }
}
