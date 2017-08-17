package org.apache.spark.sql.catalyst.expressions;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

/**
 * Created by chenfolin on 2017/8/8.
 */
public class PlFunctionInstanceGenerator {
    private PlFunctionExecutor object;

    private static Cache<String, String> functionCodes = CacheBuilder.newBuilder().expireAfterAccess(1, TimeUnit.HOURS).build();

    public PlFunctionExecutor getFuncInstance(String name){
        if(object == null){
            this.object = PlFunction.generateInstance(functionCodes.getIfPresent(name));
        }
        return object;
    }

    public static void addCode(String name, String code){
        functionCodes.put(name, code);
    }

}
