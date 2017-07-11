package org.apache.hive.tsql.ddl;

import org.antlr.v4.runtime.misc.Interval;
import org.apache.hive.basesql.func.CommonProcedureStatement;
import org.apache.hive.plsql.PlsqlParser;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

import java.util.ArrayList;

/**
 * Created by chenfl2 on 2017/7/10.
 */
public class CreateFunctionStatement extends BaseStatement {

    private static final String STATEMENT_NAME = "_CFS_";

    public PlsqlParser.Create_function_bodyContext ctx;
    private CommonProcedureStatement function;
    private SupportDataTypes returnType;
    public enum SupportDataTypes {
        STRING, VARCHAR, LONG, DOUBLE, FLOAT, INT, INTEGER, BOOLEAN, NONE
    }

    public static SupportDataTypes fromString(String str){
        if("STRING".equalsIgnoreCase(str)){
            return SupportDataTypes.STRING;
        }
        if("VARCHAR".equalsIgnoreCase(str)){
            return SupportDataTypes.VARCHAR;
        }
        if("LONG".equalsIgnoreCase(str)){
            return SupportDataTypes.LONG;
        }
        if("DOUBLE".equalsIgnoreCase(str)){
            return SupportDataTypes.DOUBLE;
        }
        if("FLOAT".equalsIgnoreCase(str)){
            return SupportDataTypes.FLOAT;
        }
        if("INT".equalsIgnoreCase(str)){
            return SupportDataTypes.INT;
        }
        if("INTEGER".equalsIgnoreCase(str)){
            return SupportDataTypes.INTEGER;
        }
        if("BOOLEAN".equalsIgnoreCase(str)){
            return SupportDataTypes.BOOLEAN;
        }
        if("NONE".equalsIgnoreCase(str)){
            return SupportDataTypes.NONE;
        }
        return null;
    }

    public enum Action {
        CREATE,REPLACE
    }

    private CreateFunctionStatement.Action create;
    private CreateFunctionStatement.Action replace;

    public CreateFunctionStatement(CommonProcedureStatement function, Action create, Action replace, SupportDataTypes returnType) {
        super(STATEMENT_NAME);
        this.function = function;
        this.create = create;
        this.replace = replace;
        this.returnType = returnType;
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
