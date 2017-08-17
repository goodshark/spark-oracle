package org.apache.spark.sql.catalyst.plfunc;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.FunctionValueExpression;
import org.apache.spark.sql.catalyst.plfunc.PlFunctionRegistry.PlFunctionDescription;
import org.apache.spark.sql.catalyst.plfunc.PlFunctionRegistry.PlFunctionIdentify;
import org.apache.spark.sql.types.IntegerType;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Created by chenfolin on 2017/8/9.
 */
public class PlFunctionUtils {

    public static String generateCode(PlFunctionIdentify id, PlFunctionRegistry registry, String enginName) {
        if("oracle".equalsIgnoreCase(enginName)){
            PlFunctionDescription func = registry.getOraclePlFunc(id);
            if(func != null){
                StringBuilder sb = new StringBuilder();
                sb.append("public Object generate(Object[] references) {\n");
                sb.append("return new ");
                sb.append(id.getDb() + "_" + id.getName());
                sb.append("(); \n}\n");
                List<PlFunctionIdentify> list = new ArrayList<>();
                getCode(sb, id, registry,list, enginName);
                return sb.toString();
            } else {
                return null;
            }
        } else {
            return null;
        }
    }

    private static void getCode(StringBuilder sb, PlFunctionIdentify id, PlFunctionRegistry functionRegistry, List<PlFunctionIdentify> exsits, String enginName){
        if("oracle".equalsIgnoreCase(enginName)){
            PlFunctionDescription func = functionRegistry.getOraclePlFunc(id);
            if(!exsits.contains(id)){
                sb.append(func.getCode());
                sb.append("\n");
                exsits.add(id);
                if(func.getChildPlfuncs() != null && func.getChildPlfuncs().size() > 0){
                    for (PlFunctionIdentify identify : func.getChildPlfuncs()){
                        getCode(sb, identify, functionRegistry, exsits, enginName);
                    }
                }
            }
        }
    }

    public static Object getExpressionInstance(String expressionClass, Integer argsSize){
        Object obj = null;
        try{
            Class c = Class.forName(expressionClass);
            Constructor cons = null;
            List<Expression> expressions = new ArrayList<>(argsSize);
            Expression[] expressions2 = new Expression[argsSize];
            Class<Expression>[] classzs = new Class[argsSize];
            for(int i = 0; i < argsSize; i++){
                expressions.add(new FunctionValueExpression(i));
                expressions2[i] = new FunctionValueExpression(i);
                classzs[i] = Expression.class;
            }
            try{
                cons = c.getDeclaredConstructor(scala.collection.Seq.class);
                if(cons != null){
                    obj = cons.newInstance(scala.collection.JavaConversions.asScalaBuffer(expressions));
                }
            } catch (Exception ex) {
                //nothing
            }
            if(obj == null){
                cons = c.getDeclaredConstructor(classzs);
                if(cons != null){
                    obj = cons.newInstance(expressions2);
                }
            }
            return obj;
        } catch (Exception e) {
            throw new RuntimeException("Can not init function " + expressionClass + ":" + e.getMessage());
        }
    }

    public static String generateCodeForTest(PlFunctionRegistry.PlFunctionDescription func, PlFunctionRegistry registry, String enginName) {
        if(func != null){
            StringBuilder sb = new StringBuilder();
            sb.append("public Object generate(Object[] references) {\n");
            sb.append("return new ");
            sb.append(func.getFunc().getDb() + "_" + func.getFunc().getName());
            sb.append("(); \n}\n");
            List<PlFunctionIdentify> list = new ArrayList<>();
            sb.append(func.getCode());
            sb.append("\n");
            list.add(func.getFunc());
            if(func.getChildPlfuncs() != null && func.getChildPlfuncs().size() > 0){
                for (PlFunctionIdentify identify : func.getChildPlfuncs()){
                    getCode(sb, identify, registry, list, enginName);
                }
            }
            return sb.toString();
        } else {
            return null;
        }
    }

}
