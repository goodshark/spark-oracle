package org.apache.hive.tsql.ddl;

import org.apache.hive.basesql.func.CommonProcedureStatement;
import org.apache.hive.plsql.PlsqlParser;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.Except;
import org.apache.spark.sql.catalyst.plfunc.PlFunctionRegistry;

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
    private SparkSession sparkSession;
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

    public CreateFunctionStatement(CommonProcedureStatement function, Action create, Action replace, SupportDataTypes returnType, SparkSession sparkSession) {
        super(STATEMENT_NAME);
        setNodeType(Type.PL_FUNCTION);
        this.function = function;
        this.create = create;
        this.replace = replace;
        this.returnType = returnType;
        this.sparkSession = sparkSession;
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
        String db = null;
        if(function.getName().getDatabase() == null) {
            db = sparkSession.getSessionState().catalog().getCurrentDatabase();
        } else {
            db = function.getName().getDatabase();
        }
        PlFunctionRegistry.PlFunctionIdentify id = new PlFunctionRegistry.PlFunctionIdentify(function.getName().getFuncName(), db);
        PlFunctionRegistry.PlFunctionDescription old = PlFunctionRegistry.getInstance().getPlFunc(id);
        if(replace == null && old != null){
            throw new Exception("Function exsits in current database.");
        } else if (old != null && old.getMd5().equals(function.getMd5())){
            return 0;
        } else {
            List<String> childPlfuncs = new ArrayList<>();
            String code = this.doCodeGen(childPlfuncs);
            List<PlFunctionRegistry.PlFunctionIdentify> children = new ArrayList<>();
            for(String str : childPlfuncs){
                if(str.contains(".")){
                    String[] ns = str.split("\\.");
                    if(ns.length != 2){
                        throw new Exception("Function call invalid. [db].[name] or [name].");
                    }
                    if(id.equals(new PlFunctionRegistry.PlFunctionIdentify(ns[1],ns[0]))){
                        throw new Exception("Can not call self function.");
                    }
                    children.add(new PlFunctionRegistry.PlFunctionIdentify(ns[1],ns[0]));
                } else {
                    if(id.equals(new PlFunctionRegistry.PlFunctionIdentify(sparkSession.getSessionState().catalog().getCurrentDatabase(), str))){
                        throw new Exception("Can not call self function.");
                    }
                    children.add(new PlFunctionRegistry.PlFunctionIdentify(sparkSession.getSessionState().catalog().getCurrentDatabase(), str));
                }
            }
            PlFunctionRegistry.getInstance().registerOrReplacePlFunc(new PlFunctionRegistry.
                    PlFunctionDescription(id, function.getProcSql(),function.getMd5(), code, returnType.toString(), children));
        }
        return 0;
    }

    public String doCodeGen(List<String> childPlfuncs) throws Exception{
        List<String> variables = new ArrayList<>();
        StringBuffer sb = new StringBuffer();
//        sb.append("public Object generate(Object[] references) {\n");
//        sb.append("return new ");
//        sb.append(function.getName().getFuncName());
//        sb.append("(); \n}\n");
        String db = null;
        if(function.getName().getDatabase() == null) {
            db = sparkSession.getSessionState().catalog().getCurrentDatabase();
        } else {
            db = function.getName().getDatabase();
        }
        sb.append("final class " + db + "_" + function.getName().getFuncName() + " implements org.apache.spark.sql.catalyst.expressions.PlFunctionExecutor{\n");
        StringBuilder sb2 = new StringBuilder();
        sb2.append("public Object eval(Object[");
        List<Var> paras = function.getInAndOutputs();
        sb2.append("] inputdatas) {\n");
        int i = 0;
        for (Var var : paras) {
            sb2.append(fromString(var.getDataType().name()).toString());
            sb2.append(CODE_SEP);
            sb2.append(var.getVarName());
            sb2.append(CODE_EQ);
            sb2.append("(");
            sb2.append(fromString(var.getDataType().name()).toString());
            sb2.append(")inputdatas[" + i + "];\n");
            i++;
        }
        List<TreeNode> children = this.function.getSqlClauses().getChildrenNodes();
        if(children != null){
            for(TreeNode node : children){
                if(node instanceof BaseStatement){
                    sb2.append(((BaseStatement) node).doCodegen(variables, childPlfuncs));
                }
            }
        }
        sb2.append("}\n");
        sb2.append("}\n");
        for(String str : variables){
            sb.append(str);
            sb.append(CODE_END);
            sb.append(CODE_LINE_END);
        }
        sb.append(sb2.toString());
        return sb.toString();
    }

}
