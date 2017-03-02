package hive.tsql.func;

import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhongdg1 on 2017/2/8.
 */
public class FunctionAliasName {

    private final static ConcurrentHashMap<String, String> functionAliasNames = new ConcurrentHashMap<>();

    private FunctionAliasName() {
        functionAliasNames.put("getdate".toUpperCase(), "now".toUpperCase());
        functionAliasNames.put("DATEADD".toUpperCase(), "date_add".toUpperCase());
        functionAliasNames.put("SYSDATETIMEOFFSET".toUpperCase(), "now".toUpperCase());
        functionAliasNames.put("CHARINDEX".toUpperCase(), "locate".toUpperCase());
        functionAliasNames.put("STRING_SPLIT".toUpperCase(), "split".toUpperCase());
        functionAliasNames.put("isnull".toUpperCase(), "ifnull".toUpperCase());
    }

    private static class FunctionAliasNameHolder {
        private final static FunctionAliasName functionAliasName = new FunctionAliasName();
    }

    public static FunctionAliasName getFunctionAlias() {
        return FunctionAliasNameHolder.functionAliasName;
    }

    public String getFunctionAliasName(String functionName) {
        String aliasName = functionAliasNames.get(functionName.toUpperCase());
        return null == aliasName ? functionName : aliasName;
    }

}
