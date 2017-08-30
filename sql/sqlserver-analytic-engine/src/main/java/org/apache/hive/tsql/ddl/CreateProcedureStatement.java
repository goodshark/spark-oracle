package org.apache.hive.tsql.ddl;

import org.apache.hive.basesql.func.CommonProcedureStatement;
import org.apache.commons.lang.StringUtils;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.dbservice.ProcService;
import org.apache.hive.tsql.func.Procedure;

/**
 * Created by zhongdg1 on 2016/12/12.
 */
public class CreateProcedureStatement extends BaseStatement {
    private static final String STATEMENT_NAME = "_CSP_";
    private CommonProcedureStatement function;
    private int type = 1;

    public enum Action {
        CREATE,ALTER
    }

    private Action action;

    public CreateProcedureStatement(CommonProcedureStatement function, Action action, int t) {
        super(STATEMENT_NAME);
        this.function = function;
        this.action=action;
        type = t;
    }

    @Override
    public int execute() throws Exception {
        // add database prefix for procedure
        if(StringUtils.isBlank(function.getName().getDatabase())){
            function.getName().setDatabase(getExecSession().getDatabase());
        }
        // oracle procedure can be created in block
        if (getExecSession().getCurrentScope() != null) {
            addFunc(function);
            return 0;
        }
        switch (action){
            case CREATE:
                // 运行时才将PROC加入到变量容器的VariableContainer.functions map
                // 在内存中的proc需要保存在数据库中
                if(function.getProcSource()==0){
                    ProcService procService = new ProcService(getExecSession().getSparkSession());
                    if(StringUtils.isBlank(function.getName().getDatabase())){
                        function.getName().setDatabase(getExecSession().getDatabase());
                    }
                    procService.createProc(function, type);
                }
                break;
            case ALTER:
                ProcService procService = new ProcService(getExecSession().getSparkSession());
                if(StringUtils.isBlank(function.getName().getDatabase())){
                    function.getName().setDatabase(getExecSession().getDatabase());
                }
                String procName=function.getName().getRealFullFuncName();
                int count= procService.getCountByName(procName, type);
                if(count==0){
                    throw  new Exception(procName + " is exist;");
                }else{
                    procService.updateProc(function, type);
                }
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
