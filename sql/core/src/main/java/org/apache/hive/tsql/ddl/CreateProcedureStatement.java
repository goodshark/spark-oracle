package org.apache.hive.tsql.ddl;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.dbservice.ProcService;
import org.apache.hive.tsql.func.Procedure;

/**
 * Created by zhongdg1 on 2016/12/12.
 */
public class CreateProcedureStatement extends BaseStatement {
    private static final String STATEMENT_NAME = "_CSP_";
    private Procedure function;

    public enum Action {
        CREATE,ALTER
    }

    private Action action;

    public CreateProcedureStatement(Procedure function,Action action) {
        super(STATEMENT_NAME);
        this.function = function;
        this.action=action;
    }

    @Override
    public int execute() throws Exception {

        switch (action){
            case CREATE:
                /**
                 * 运行时才将PROC加入到变量容器的VariableContainer.functions map
                 */
                //在内存中的proc需要保存在数据库中
                if(function.getProcSource()==0){
                    ProcService procService = new ProcService(getExecSession().getSparkSession());
                    procService.createProc(function);
                }
                break;
            case ALTER:
                ProcService procService = new ProcService(getExecSession().getSparkSession());
                String procName=function.getName().getFullFuncName();
                int count= procService.getCountByName(procName);
                if(count>0){
                    procService.delProc(procName);
                }
                procService.createProc(function);
                break;
        }
        super.addFunc(function);
        return 0;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public String getSql() {
        return function.getProcSql();
    }
}
