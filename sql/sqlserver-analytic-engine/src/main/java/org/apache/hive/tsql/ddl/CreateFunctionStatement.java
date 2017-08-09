package org.apache.hive.tsql.ddl;

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
        List<String> variables = new ArrayList<>();
        List<String> childPlfuncs = new ArrayList<>();
        StringBuffer sb = new StringBuffer();
        sb.append("public Object generate(Object[] references) {\n");
        sb.append("return new ");
        sb.append(function.getName().getFuncName());
        sb.append("(); \n}\n");

        sb.append("final class " + function.getName().getFuncName() + " implements org.apache.spark.sql.catalyst.expressions.PlFunctionExecutor{\n");
        sb.append("public Object eval(Object[");
        List<Var> paras = function.getInAndOutputs();
        sb.append("] inputdatas) {\n");
        int i = 0;
        for (Var var : paras) {
            sb.append(fromString(var.getDataType().name()).toString());
            sb.append(CODE_SEP);
            sb.append(var.getVarName());
            sb.append(CODE_EQ);
            sb.append("(");
            sb.append(fromString(var.getDataType().name()).toString());
            sb.append(")inputdatas[" + i + "];\n");
            i++;
        }
        List<TreeNode> children = this.function.getSqlClauses().getChildrenNodes();
        if(children != null){
            for(TreeNode node : children){
                if(node instanceof BaseStatement){
                    sb.append(((BaseStatement) node).doCodegen(variables, childPlfuncs));
                }
            }
        }
        sb.append("}\n");
        sb.append("}\n");
        return sb.toString();
    }

    private String genDeclare(){
        StringBuffer sb = new StringBuffer();
        return sb.toString();
    }

}
