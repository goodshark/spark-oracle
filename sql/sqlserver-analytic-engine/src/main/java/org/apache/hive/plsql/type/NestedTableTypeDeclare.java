package org.apache.hive.plsql.type;

import org.apache.hive.tsql.arg.Var;

/**
 * Created by dengrb1 on 8/1 0001.
 */
public class NestedTableTypeDeclare extends LocalTypeDeclare {
    public NestedTableTypeDeclare() {
        super(Var.DataType.NESTED_TABLE);
    }

    public Var.DataType getNestedTableValueType() {
        return tableTypeVar.getDataType();
    }

    @Override
    public int execute() throws Exception {
        if (tableTypeVar.getDataType() == Var.DataType.REF_SINGLE)
            resolveRefSingle(tableTypeVar);
        if (tableTypeVar.getDataType() == Var.DataType.REF_COMPOSITE)
            resolveRefComposite(tableTypeVar);
        if (tableTypeVar.getDataType() == Var.DataType.CUSTOM)
            resolveCustomType(tableTypeVar);
        addType(this);
        return 0;
    }
}
