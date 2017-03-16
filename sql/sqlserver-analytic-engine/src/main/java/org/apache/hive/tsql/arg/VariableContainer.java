package org.apache.hive.tsql.arg;

import org.apache.hive.tsql.cursor.Cursor;
import org.apache.hive.tsql.func.Procedure;

import java.text.ParseException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhongdg1 on 2016/12/8.
 * 一般变量容器
 */
public class VariableContainer {

    //保存变量, 作用域仅限为go
    private ConcurrentHashMap<String, Var> vars = new ConcurrentHashMap<String, Var>();
    //保存表变量,作用域一个GO
    private ConcurrentHashMap<String, Var> tableVars = new ConcurrentHashMap<String, Var>();
    //保存临时表，作用域多个GO之间,<##temp, AliasName>
    private ConcurrentHashMap<String, String> tmpTables = new ConcurrentHashMap<String, String>();

    //保存func/proc
    private ConcurrentHashMap<String, Procedure> functions = new ConcurrentHashMap<String, Procedure>();
    //保存cursor
    private ConcurrentHashMap<String, Cursor> localCursors = new ConcurrentHashMap<>();//本地游标
    private ConcurrentHashMap<String, Cursor> globalCursors = new ConcurrentHashMap<>();//全局游标
    //系统变量
    private ConcurrentHashMap<String,Var> systemVariables = new ConcurrentHashMap<>();

    public VariableContainer() {
        //init system variables
        addOrUpdateSys(new Var(SystemVName.FETCH_STATUS, 0, Var.DataType.INT));
        addOrUpdateSys(new Var(SystemVName.CURSOR_ROWS, 0, Var.DataType.INT));
    }

    public void addTableVars(Var var) {
        tableVars.put(var.getVarName().toUpperCase(), var);
    }

    public String findTableVarAlias(String tableVarName) {
        Var v = tableVars.get(tableVarName.toUpperCase());
        return null == v ? null : v.getAliasName();
    }

    public void addTmpTable(String tmpTable, String aliasName) {
        tmpTables.put(tmpTable, aliasName);
    }

    public String findTmpTaleAlias(String tmpTableName) {
        return tmpTables.get(tmpTableName);
    }

    public void deleteTmpTable(String tmp) {
        tmpTables.remove(tmp);
    }

    public Var findSystemVar(String sysName) {
        return systemVariables.get(sysName);
    }

    //按指定容器查找
    public Cursor findCursor(String name, boolean isGlobal) {
        name = name.toUpperCase();
        return isGlobal ? globalCursors.get(name) : localCursors.get(name);
    }

    //local存在在返回，否则找global
    public Cursor findCursor(String name) {
        name = name.toUpperCase();
        return localCursors.get(name) != null ? localCursors.get(name) : globalCursors.get(name);
    }


    //保持只有一个Cursor，LOCAL OR GLOBAL
    public Cursor addCursor(Cursor cursor) {
        return cursor.isGlobal() ? globalCursors.put(cursor.getName(), cursor) : localCursors.put(cursor.getName(), cursor);
    }

    //如果指定global则直接delete global，否则优先delete local
    public void deleteCursor(String cursorName, boolean isGlobal) {
        cursorName = cursorName.toUpperCase();
        if (isGlobal) {
            globalCursors.remove(cursorName);
            return;
        }
        if (null != localCursors.get(cursorName)) {
            localCursors.remove(cursorName);
            return;
        }
        globalCursors.remove(cursorName);
    }

    private Var returnVar = new Var("_RETURN_", Var.DataType.INT);


    public void setReturnVar(Var returnVar) {
        this.returnVar = returnVar;
    }

    public Var getReturnVar() {
        return returnVar;
    }

    public void setReturnVal(int returnVal) {
        this.returnVar.setVarValue(returnVal);
        this.returnVar.setExecuted(true);
    }

    public Var setVarValue(String varName, Object val) {
        Var var = findVar(varName.toLowerCase());
        var.setVarValue(val);
        var.setExecuted(true);
        return var;
    }



    public void addProcFunc(Procedure function) {
        this.functions.put(function.getName().getFullFuncName(), function);
    }

    public ConcurrentHashMap<String, Var> getVars() {
        return vars;
    }

    public void addVar(Var var) {
        vars.put(var.getVarName(), var);
    }

    public void setVars(ConcurrentHashMap<String, Var> vars) {
        this.vars = vars;
    }

    public void updateVarValue(Var var) throws ParseException {
        Var oldVar = findVar(var);
        if (null != oldVar) {
            oldVar.setVarValue(var.getVarValue());
        }

    }

    /**
     * 按变量名查找，找不到按变量别名查找
     *
     * @param var
     * @return
     */
    public Var findVar(Var var) {

        return null == vars.get(var.getVarName().toUpperCase()) ? vars.get(var.getAliasName()) : vars.get(var.getVarName().toUpperCase());
    }

    public Var findVar(String varName) {
        return vars.get(varName.toUpperCase());
    }

    public Procedure findFunc(String varName) {
        return functions.get(varName);
    }

    public Var updateValue(String name, Object val) {
        Var v = findVar(name);
        v.setVarValue(val);
        v.setExecuted(true);
        return v;
    }

    public Set<String> getAllTableVarNames() {
        return this.tableVars.keySet();
    }


    public Set<String> getAllTmpTableNames() {
        return this.tmpTables.keySet();
    }

    public void deleteVar(String varName) {
        this.vars.remove(varName.toUpperCase());
    }

    public void resetVars() {
        this.vars = new ConcurrentHashMap<String, Var>();
        this.tableVars = new ConcurrentHashMap<String,Var>();
        this.localCursors = new ConcurrentHashMap<String, Cursor>();
    }

    public Var addOrUpdateSys(Var var) {
        var.setExecuted(true);
        return systemVariables.put(var.getVarName().toUpperCase(), var);
    }

    public boolean updateSys(String sysVarName, Object val) {
        Var v = systemVariables.get(sysVarName.toUpperCase());
        if(null == v) {
            return false;
        }
        v.setVarValue(val);
        v.setExecuted(true);
        return true;
    }

}