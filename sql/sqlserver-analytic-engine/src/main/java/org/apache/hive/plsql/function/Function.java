package org.apache.hive.plsql.function;

import org.apache.hive.basesql.func.CommonProcedureStatement;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.func.FuncName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 5/25 0025.
 */
public class Function extends CommonProcedureStatement implements Serializable {
    // TODO return type

    public Function(FuncName name) {
        this(name, true, null);
    }

    public Function(FuncName name, boolean global, String hash) {
        super(name, global, hash);
    }
}
