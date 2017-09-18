package org.apache.hive.plsql.type;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by dengrb1 on 8/1 0001.
 */
public abstract class LocalTypeDeclare extends BaseStatement {
    private static final String STATEMENT_NAME = "_LOCAL_TYPE_DECLARE_";
    // COMPOSITE | VARRAY | ASSOC_ARRAY | NESTED_TABLE
    private Var.DataType type = null;

    public enum Type {
        RECORD, TABLE
    }
    private Type declareType = Type.RECORD;
    private String typeName = "";
    protected Map<String, Var> typeVarMap = new HashMap<>();
    protected Var tableTypeVar = null;
    private boolean resolved = false;

    public LocalTypeDeclare() {
        super(STATEMENT_NAME);
    }

    /*public LocalTypeDeclare(Type t) {
        super(STATEMENT_NAME);
        declareType = t;
    }*/

    public LocalTypeDeclare(Var.DataType t) {
        super(STATEMENT_NAME);
        type = t;
    }

    public void setTypeName(String t) {
        typeName = t;
    }

    public String getTypeName() {
        return typeName;
    }

    public Var.DataType getDeclareType() {
        return type;
    }

    public void addTypeVar(String fieldName, Var v) {
        typeVarMap.put(fieldName, v);
    }

    public Map<String, Var> getTypeVars() {
        return new HashMap<String, Var>(typeVarMap);
    }

    public void setTableTypeVar(Var v) {
        tableTypeVar = v;
    }

    public Var getTableTypeVar() {
        return tableTypeVar.clone();
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }

    public void setResolved() {
        resolved = true;
    }

    public boolean isResolved() {
        return resolved;
    }

    /*@Override
    public int execute() throws Exception {
        // TODO resolve all types
        for (String varName: typeVarMap.keySet()) {
            Var typeVar = typeVarMap.get(varName);
            if (typeVar.getDataType() == Var.DataType.REF_SINGLE)
                return -1;
            if (typeVar.getDataType() == Var.DataType.REF_COMPOSITE)
                return -1;
            if (typeVar.getDataType() == Var.DataType.CUSTOM)
                return -1;
        }
        return 0;
    }*/
}
