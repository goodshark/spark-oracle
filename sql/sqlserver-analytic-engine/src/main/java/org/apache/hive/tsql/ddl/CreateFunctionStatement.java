package org.apache.hive.tsql.ddl;

import javolution.io.Struct;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.hive.basesql.func.CommonProcedureStatement;
import org.apache.hive.plsql.PlsqlParser;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chenfl2 on 2017/7/10.
 */
public class CreateFunctionStatement extends BaseStatement {

    private static final String STATEMENT_NAME = "_CFS_";

    public PlsqlParser.Create_function_bodyContext ctx;
    private CommonProcedureStatement function;
    private SupportDataTypes returnType;
    public enum SupportDataTypes {
        STRING, CHAR,VARCHAR,VARCHAR2, LONG, DOUBLE, FLOAT, INT, INTEGER, BOOLEAN, NONE;

        @Override
        public String toString() {
            switch (this) {
                case INT:return "Integer";
                case LONG:return "Long";
                case FLOAT:return "Float";
                case DOUBLE:return "Double";
                case STRING:return "String";
                case BOOLEAN:return "Boolean";
                case INTEGER:return "Integer";
                case CHAR:return "String";
                case VARCHAR:return "String";
                case VARCHAR2:return "String";
                case NONE:return "Void";
            }
            return super.toString();
        }
    }

    public static SupportDataTypes fromString(String str){
        if("STRING".equalsIgnoreCase(str)){
            return SupportDataTypes.STRING;
        }
        if("CHAR".equalsIgnoreCase(str)){
            return SupportDataTypes.CHAR;
        }
        if("VARCHAR".equalsIgnoreCase(str)){
            return SupportDataTypes.VARCHAR;
        }
        if("VARCHAR2".equalsIgnoreCase(str)){
            return SupportDataTypes.VARCHAR2;
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

    public String doCodeGen() {
        StringBuffer sb = new StringBuffer();
        sb.append("private ");
        sb.append(returnType.toString());
        sb.append(" eval(");
        List<Var> paras = function.getInAndOutputs();
        int i = 0;
        for (Var var : paras) {
            i++;
            sb.append(fromString(var.getValueType().name()).toString());
            sb.append(" ");
            sb.append(var.getVarName());
            if(i != paras.size()){
                sb.append(",");
            }
        }
        sb.append(") {\n");
        return sb.toString();
    }

    private String genDeclare(){
        StringBuffer sb = new StringBuffer();
        return sb.toString();
    }

}
