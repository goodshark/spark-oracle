package org.apache.hive.tsql.ddl;

import org.antlr.v4.runtime.misc.Interval;
import org.apache.hive.basesql.func.CommonProcedureStatement;
import org.apache.hive.plsql.PlsqlParser;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

/**
 * Created by chenfl2 on 2017/7/10.
 */
public class CreateFunctionStatement extends BaseStatement {

    private static final String STATEMENT_NAME = "_CFS_";

    public PlsqlParser.Create_function_bodyContext ctx;
    private String sourceSql;
    private CommonProcedureStatement function;

    public enum Action {
        CREATE,REPLACE
    }

    private CreateFunctionStatement.Action create;
    private CreateFunctionStatement.Action replace;

    public CreateFunctionStatement(CommonProcedureStatement function, Action create, Action replace) {
        super(STATEMENT_NAME);
        this.function = function;
        this.create = create;
        this.replace = replace;
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }

    @Override
    public String getSql() {
        return function.getProcSql();
    }

    @Override
    public int execute() throws Exception{
        return 0;
    }

}
