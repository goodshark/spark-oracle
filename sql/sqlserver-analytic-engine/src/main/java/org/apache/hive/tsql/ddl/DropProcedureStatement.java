package org.apache.hive.tsql.ddl;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.tsql.common.SqlStatement;
import org.apache.hive.tsql.dbservice.ProcService;
import org.apache.hive.tsql.func.FuncName;

import java.util.List;

/**
 * Created by zhongdg1 on 2016/12/15.
 */
public class DropProcedureStatement extends SqlStatement {

    private List<FuncName> funcNames;

    @Override
    public int execute() throws Exception {
        ProcService procService = new ProcService(getExecSession().getSparkSession());
        for (FuncName f : funcNames) {
            if (StringUtils.isBlank(f.getDatabase())) {
                f.setDatabase(getExecSession().getDatabase());
            }
            if (procService.getCountByName(f.getRealFullFuncName()) > 0) {
                procService.delProc(f.getRealFullFuncName());
            } else {
                throw new Exception("Proc: " + f.getRealFullFuncName() + " NOT FIND .");
            }

        }
        return 0;
    }

    public DropProcedureStatement(List<FuncName> funcNames) {
        this.funcNames = funcNames;
    }
}
