package org.apache.spark.sql.catalyst.plfunc;

import org.apache.spark.sql.catalyst.plfunc.PlFunctionRegistry.PlFunctionDescription;
import org.apache.spark.sql.catalyst.plfunc.PlFunctionRegistry.PlFunctionIdentify;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chenfolin on 2017/8/9.
 */
public class PlFunctionUtils {

    public static String generateCode(PlFunctionIdentify id) {
        PlFunctionDescription func = PlFunctionRegistry.getInstance().getPlFunc(id);
        if(func != null){
            StringBuilder sb = new StringBuilder();
            sb.append("public Object generate(Object[] references) {\n");
            sb.append("return new ");
            sb.append(id.getDb() + "_" + id.getName());
            sb.append("(); \n}\n");
            List<PlFunctionIdentify> list = new ArrayList<>();
            getCode(sb, id, PlFunctionRegistry.getInstance(),list);
            return sb.toString();
        } else {
            return null;
        }
    }

    private static String getCode(StringBuilder sb, PlFunctionIdentify id, PlFunctionRegistry functionRegistry, List<PlFunctionIdentify> exsits){
        PlFunctionDescription func = functionRegistry.getPlFunc(id);
        if(!exsits.contains(id)){
            sb.append(func.getCode());
            sb.append("\n");
            exsits.add(id);
            if(func.getChildPlfuncs() != null && func.getChildPlfuncs().size() > 0){
                for (PlFunctionIdentify identify : func.getChildPlfuncs()){
                    getCode(sb, identify, functionRegistry, exsits);
                }
            }
        }
        return sb.toString();
    }

}
