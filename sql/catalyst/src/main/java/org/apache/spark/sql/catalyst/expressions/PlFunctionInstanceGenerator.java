package org.apache.spark.sql.catalyst.expressions;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by chenfolin on 2017/8/8.
 */
public class PlFunctionInstanceGenerator {
    private PlFunctionExecutor object;

    private static Map<String, String> functionCodes = new HashMap<>();

    public PlFunctionExecutor getFuncInstance(String name){
        if(object == null){
            this.object = PlFunction.generateInstance(functionCodes.get(name));
        }
        return object;
    }

    public static void addCode(String name, String code){
        functionCodes.put(name, code);
    }

}
