package org.apache.hive.tsql.arg;

import org.apache.hive.basesql.cursor.CommonCursor;
import org.apache.hive.basesql.func.CommonProcedureStatement;
import org.apache.hive.plsql.type.LocalTypeDeclare;
import org.apache.hive.tsql.ExecSession;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dbservice.ProcService;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.Except;

import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hive.tsql.execute.Executor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhongdg1 on 2016/12/8.
 * 一般变量容器
 */
public class VariableContainer {
    private static final Logger LOG = LoggerFactory.getLogger(VariableContainer.class);

    //保存变量, 作用域仅限为go
    private ConcurrentHashMap<String, Var> vars = new ConcurrentHashMap<String, Var>();
    private ConcurrentHashMap<TreeNode, ConcurrentHashMap<String, Var>> newVars = new ConcurrentHashMap<>();
    // vars in oracle package only
    private final int PACKAGE_TYPE = 5;
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Var>> packageVars = new ConcurrentHashMap<>();
    //保存表变量,作用域一个GO
    private ConcurrentHashMap<String, Var> tableVars = new ConcurrentHashMap<String, Var>();
    //保存临时表，作用域多个GO之间,<##temp, AliasName>
    private ConcurrentHashMap<String, String> tmpTables = new ConcurrentHashMap<String, String>();

    //保存func/proc
    private ConcurrentHashMap<String, CommonProcedureStatement> functions = new ConcurrentHashMap<String, CommonProcedureStatement>();
    private ConcurrentHashMap<TreeNode, ConcurrentHashMap<String, List<CommonProcedureStatement>>> newFunctions = new ConcurrentHashMap<>();
    // func/proc in oracle package only
    private ConcurrentHashMap<String, ConcurrentHashMap<String, CommonProcedureStatement>> packageFunctions = new ConcurrentHashMap<>();
    //保存cursor
    private ConcurrentHashMap<String, CommonCursor> localCursors = new ConcurrentHashMap<>();//本地游标
    private ConcurrentHashMap<TreeNode, ConcurrentHashMap<String, CommonCursor>> newLocalCursors =
            new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, CommonCursor> globalCursors = new ConcurrentHashMap<>();//全局游标
    //系统变量
    private ConcurrentHashMap<String,Var> systemVariables = new ConcurrentHashMap<>();
    // custom type
    private ConcurrentHashMap<TreeNode, ConcurrentHashMap<String, LocalTypeDeclare>> types = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, ConcurrentHashMap<String, LocalTypeDeclare>> packageTypes = new ConcurrentHashMap<>();

    private ExecSession session;

    private ProcService dbService;

    public VariableContainer(ExecSession ss) {
        //init system variables
        addOrUpdateSys(new Var(SystemVName.FETCH_STATUS, 0, Var.DataType.INT));
        addOrUpdateSys(new Var(SystemVName.CURSOR_ROWS, 0, Var.DataType.INT));
        session = ss;
        dbService = new ProcService(ss.getSparkSession());
    }

    private ConcurrentHashMap<String, Var> getNormalVars() {
        return null;
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
        return systemVariables.get(sysName.toUpperCase());
    }

    //按指定容器查找
    public CommonCursor findCursor(String name, boolean isGlobal) {
        name = name.toUpperCase();
        return isGlobal ? getGlobalCursor(name) : getLocalCursor(name);
//        return isGlobal ? globalCursors.get(name) : localCursors.get(name);
    }

    //local存在在返回，否则找global
    public CommonCursor findCursor(String name) {
        name = name.toUpperCase();
        CommonCursor cursor = getLocalCursor(name);
        return cursor == null ? getGlobalCursor(name) : cursor;
//        return localCursors.get(name) != null ? localCursors.get(name) : globalCursors.get(name);
    }

    private CommonCursor getLocalCursor(String name) {
        TreeNode[] blocks = session.getCurrentScopes();
        ConcurrentHashMap<String, CommonCursor> mapCursor = null;
        for (TreeNode curBlock: blocks) {
            mapCursor = newLocalCursors.get(curBlock);
            if (mapCursor != null && mapCursor.get(name) != null)
                return mapCursor.get(name);
        }
        TreeNode rootBlock = session.getRootNode();
        mapCursor = newLocalCursors.get(rootBlock);
        if (mapCursor != null)
            return mapCursor.get(name);
        return null;
    }

    private CommonCursor getGlobalCursor(String name) {
        return globalCursors.get(name);
    }


    //保持只有一个Cursor，LOCAL OR GLOBAL
    public void addCursor(CommonCursor cursor) {
//        return cursor.isGlobal() ? globalCursors.put(cursor.getName(), cursor) : localCursors.put(cursor.getName(), cursor);
        if (cursor.isGlobal())
            addGlobalCursor(cursor);
        else
            addLocalCursor(cursor);
    }

    private void addGlobalCursor(CommonCursor cursor) {
        globalCursors.put(cursor.getName(), cursor);
    }

    private void addLocalCursor(CommonCursor cursor) {
        TreeNode curBlock = session.getCurrentScope();
        ConcurrentHashMap<String, CommonCursor> mapCursor = new ConcurrentHashMap<>();
        mapCursor.put(cursor.getName().toUpperCase(), cursor);
        if (curBlock != null) {
            if (newLocalCursors.containsKey(curBlock))
                newLocalCursors.get(curBlock).put(cursor.getName(), cursor);
            else
                newLocalCursors.put(curBlock, mapCursor);
        } else {
            if (newLocalCursors.containsKey(session.getRootNode()))
                newLocalCursors.get(session.getRootNode()).put(cursor.getName(), cursor);
            else
                newLocalCursors.put(session.getRootNode(), mapCursor);
        }
//        localCursors.put(cursor.getName(), cursor);
    }

    //如果指定global则直接delete global，否则优先delete local
    public void deleteCursor(String cursorName, boolean isGlobal) {
        // TODO delete cursor in hierachy block
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

    private Var funcReturnVar = null;

    public void setFuncReturnVar(Var res) {
        funcReturnVar = res;
    }

    public Var getFuncReturnVar() {
        return funcReturnVar;
    }

    public void clearFuncReturnVar() {
        funcReturnVar = null;
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

    public void addProcFunc(CommonProcedureStatement function) {
        if (function == null)
            return;
        String functionName = function.getName().getFullFuncName().toUpperCase();
        if (session.isPackageScope()) {
            if (!packageFunctions.containsKey(session.getPackageName()))
                packageFunctions.put(session.getPackageName(), new ConcurrentHashMap<String, CommonProcedureStatement>());
            packageFunctions.get(session.getPackageName()).put(functionName, function);
            return;
        }

        TreeNode curBlock = session.getCurrentScope();
        if (curBlock != null) {
            if (newFunctions.containsKey(curBlock)) {
                if (newFunctions.get(curBlock).containsKey(functionName)) {
                    newFunctions.get(curBlock).get(functionName).add(function);
                } else {
                    List<CommonProcedureStatement> procList = new ArrayList<>();
                    procList.add(function);
                    newFunctions.get(curBlock).put(functionName, procList);
                }
            } else {
                ConcurrentHashMap<String, List<CommonProcedureStatement>> procMap = new ConcurrentHashMap<>();
                List<CommonProcedureStatement> procList = new ArrayList<>();
                procList.add(function);
                procMap.put(functionName, procList);
                newFunctions.put(curBlock, procMap);
            }
        } else {
            if (newFunctions.containsKey(session.getRootNode())) {
                if (newFunctions.get(session.getRootNode()).containsKey(functionName)) {
                    newFunctions.get(session.getRootNode()).get(functionName).add(function);
                } else {
                    List<CommonProcedureStatement> procList = new ArrayList<>();
                    procList.add(function);
                    newFunctions.get(session.getRootNode()).put(functionName, procList);
                }
            } else {
                ConcurrentHashMap<String, List<CommonProcedureStatement>> procMap = new ConcurrentHashMap<>();
                List<CommonProcedureStatement> procList = new ArrayList<>();
                procList.add(function);
                procMap.put(functionName, procList);
                newFunctions.put(session.getRootNode(), procMap);
            }
        }
//        this.functions.put(function.getName().getFullFuncName(), function);

    }

    public ConcurrentHashMap<String, Var> getVars() {
        return vars;
    }

    public void addVar(Var var) {
        if (session.isPackageScope()) {
            LOG.info("variable container add var in package scope: " + session.getPackageName() + ", var: " + var.getVarName());
            if (!packageVars.containsKey(session.getPackageName())) {
                ConcurrentHashMap<String, Var> tmpMap = new ConcurrentHashMap<>();
                packageVars.put(session.getPackageName(), tmpMap);
            }
            packageVars.get(session.getPackageName()).put(var.getVarName(), var);
            return;
        }
        TreeNode curBlock = session.getCurrentScope();
        if (curBlock != null) {
            if (newVars.containsKey(curBlock)) {
                newVars.get(curBlock).put(var.getVarName(), var);
            } else {
                ConcurrentHashMap<String, Var> varMap = new ConcurrentHashMap<String, Var>();
                varMap.put(var.getVarName(), var);
                newVars.put(curBlock, varMap);
            }
        } else {
            if (newVars.containsKey(session.getRootNode())) {
                newVars.get(session.getRootNode()).put(var.getVarName(), var);
            } else {
                ConcurrentHashMap<String, Var> varMap = new ConcurrentHashMap<String, Var>();
                varMap.put(var.getVarName(), var);
                newVars.put(session.getRootNode(), varMap);
            }
        }
//        vars.put(var.getVarName(), var);
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

    public ConcurrentHashMap<String, ConcurrentHashMap<String, Var>> getPackageVars() {
        return packageVars;
    }

    private boolean loadPackFromSc(String packName) {
        SparkSession ss = session.getSparkSession();
        LOG.info("variable container load package info from sc, ss: " + ss + ", hash: " + ss.hashCode() + ", id: " + ss.sessionId());
        SparkContext sc = ss.sparkContext();
        Map<String, ConcurrentHashMap<String, Object>> tmpPackMap = sc.oraclePackageVars().get(ss.sessionId());
        if (tmpPackMap == null) {
            LOG.info("variable container load package from sc, ss bind val is null");
            return false;
        } else {
            LOG.info("variable container load package from sc, ss bind val is not null");
            ConcurrentHashMap<String, Object> tmpVarMap = tmpPackMap.get(packName.toUpperCase());
            if (tmpVarMap == null) {
                LOG.info("variable container load package from sc, package name bind val is null");
                return false;
            } else {
                LOG.info("variable container load package from sc, package name bind val is not null");
                // transform Object-Vars into Vars
                ConcurrentHashMap<String, Var> tranMap = new ConcurrentHashMap<>();
                for (String varName: tmpVarMap.keySet()) {
                    LOG.info("variable container load package from sc, var name: " + varName + ", value: " + tmpVarMap.get(varName));
                    tranMap.put(varName, (Var)(tmpVarMap.get(varName)));
                }
                packageVars.put(packName.toUpperCase(), tranMap);
                return true;
            }
        }
    }

    private boolean loadPackFromDb(String packName) {
        try {
            LOG.info("variable container load package info from db");
            List<TreeNode> treeNodes = dbService.getPackageObj(packName, PACKAGE_TYPE);
            if (treeNodes == null)
                return false;
            // restrict all vars into package scope
            session.setPackageScope(packName);
            for (TreeNode treeNode: treeNodes)
                new Executor(session, treeNode).run();
        } catch (Exception e) {
            e.printStackTrace();
            session.clearePackageScope();
            return false;
        }
        session.clearePackageScope();
        return true;
    }

    private Var findVarInPackage(String packageName, String vName, String[] tags) {
        boolean loadSuccess = false;
        if (!packageVars.containsKey(packageName.toUpperCase())) {
            loadSuccess = loadPackFromSc(packageName.toUpperCase()) || loadPackFromDb(packageName.toUpperCase());
        }
        LOG.info("package vars load status: " + loadSuccess);
        if (packageVars.containsKey(packageName) || loadSuccess) {
            LOG.info("package vars already in local packageVars");
            return searchDotVar(true, null, tags);
            /*ConcurrentHashMap<String, Var> pack = packageVars.get(packageName.toUpperCase());
            // TODO testonly
            LOG.info("package vars in package name: " + packageName.toUpperCase());
            for (Var v: pack.values()) {
                LOG.info("package var name: " + v.getVarName() + ", vName: " + vName);
            }
            if (pack.containsKey(vName))
                return pack.get(vName);
            else
                return null;*/
        } else {
            return null;
        }
    }

    private Var searchDotVar(boolean packageScope, Var rootVar, String[] tagNames, Object ...args) {
        Var curVar = rootVar;
        Var methodVar = null;
        for (int i = 0; i < tagNames.length; i++) {
            if (i == 0 && rootVar == null) {
                if (session.isPackageScope()) {
                    // only for complex-var in package
                    ConcurrentHashMap<String, Var> packVarMap = packageVars.get(session.getPackageName());
                    if (packVarMap == null)
                        break;
                    curVar = packVarMap.get(tagNames[i].toUpperCase());
                } else if (packageScope) {
                    ConcurrentHashMap<String, Var> packVarMap = packageVars.get(tagNames[i].toUpperCase());
                    if (packVarMap == null)
                        break;
                    curVar = packVarMap.get(tagNames[++i].toUpperCase());
                } else{
                    curVar = getVarInBlocks(tagNames[i].toUpperCase());
                }
                if (curVar == null) {
                    break;
                }
            } else {
                if (curVar != null) {
                    /*if (tagNames[i].equalsIgnoreCase("count") && curVar.getDataType() == Var.DataType.NESTED_TABLE) {
                        return new Var("", curVar.getArraySize(), Var.DataType.INTEGER);
                    }*/
                    try {
                        methodVar = Var.callCollectionMethod(curVar, tagNames[i], args);
                    } catch (Exception e) {
                        // do nothing
                    }
                    if (methodVar == null)
                        curVar = curVar.getInnerVar(tagNames[i].toUpperCase());
                    else
                        return methodVar;
                } else {
                    return null;
                }
            }
        }
        return curVar;
    }

    /**
     * 按变量名查找，找不到按变量别名查找
     *
     * @param var
     * @return
     */
    public Var findVar(Var var) {
        // TODO
        if (var.getVarName() == null) {
            return vars.get(var.getAliasName());
        } else {
            return findVar(var.getVarName());
        }
//        return null == vars.get(var.getVarName().toUpperCase()) ? vars.get(var.getAliasName()) : vars.get(var.getVarName().toUpperCase());
    }

    // need "." more than 1 for x.y.z, the first tag maybe scope-name
    public Var findVar(String varName, Object ...args) {
        /*String scopeName = "";
        if (varName.contains(".")) {
            String[] fullVarArray = varName.split("\\.");
            // ignore . counts more than 2
            if (fullVarArray.length == 2) {
                scopeName = fullVarArray[0];
                varName = fullVarArray[1];
            }
        }
        // first use scope name as var name to search
        Var prefixVar = getVarInBlocks(scopeName.toUpperCase());
        if (prefixVar != null) {
            Var innerVar = prefixVar.getInnerVar(varName.toUpperCase());
            if (innerVar != null)
                return innerVar;
        }
        TreeNode[] blocks = session.getCurrentScopes();
        // search all nested scope
        for (TreeNode blk: blocks) {
            if (!scopeName.isEmpty()) {
                BaseStatement stmt = (BaseStatement) blk;
                if (!stmt.existLabel(scopeName))
                    continue;
            }
            ConcurrentHashMap<String, Var> blkVars = newVars.get(blk);
            if (blkVars != null && blkVars.get(varName.toUpperCase()) != null) {
                return blkVars.get(varName.toUpperCase());
            } else {
                if (!scopeName.isEmpty())
                    return null;
            }
        }
        return getVarInGlobal(varName);*/
        String scopeName = "";

        String[] fullVarArray = varName.split("\\.");
        Var targetVar = searchDotVar(false, null, fullVarArray, args);
        if (targetVar != null)
            return targetVar;

        if (fullVarArray.length <= 1)
            return getVarInGlobal(varName);
        else {
            TreeNode[] blocks = session.getCurrentScopes();
            scopeName = fullVarArray[0];
            varName = fullVarArray[1];
            for (TreeNode blk : blocks) {
                if (!scopeName.isEmpty()) {
                    BaseStatement stmt = (BaseStatement) blk;
                    if (!stmt.existLabel(scopeName))
                        continue;
                }
                ConcurrentHashMap<String, Var> blkVars = newVars.get(blk);
                if (blkVars != null && blkVars.get(varName.toUpperCase()) != null) {
                    Var rootVar = blkVars.get(varName.toUpperCase());
                    return searchDotVar(false, rootVar, Arrays.copyOfRange(fullVarArray, 2, fullVarArray.length));
                } else {
                    if (!scopeName.isEmpty())
                        return null;
                }
            }
            // x.y, x is the package name
            Var packageVar = findVarInPackage(scopeName, varName.toUpperCase(), fullVarArray);
            return packageVar;
        }
    }

    private Var getVarInGlobal(String varName) {
        // search global scope
        ConcurrentHashMap<String, Var> rootScope = newVars.get(session.getRootNode());
        if (rootScope != null) {
            return rootScope.get(varName.toUpperCase());
        }
        return null;
    }

    private Var getVarInBlocks(String varName) {
        TreeNode[] blocks = session.getCurrentScopes();
        for (TreeNode blk: blocks) {
            ConcurrentHashMap<String, Var> blkVars = newVars.get(blk);
            if (blkVars != null && blkVars.get(varName.toUpperCase()) != null) {
                return blkVars.get(varName.toUpperCase());
            }
        }
        return getVarInGlobal(varName);
    }

    private CommonProcedureStatement findProcedureInPackage(String name) {
        // procedure inside package will rename as x.y.z => DB.PACKAGE.NAME
        try {
            String[] tags = name.split("\\.");
            if (tags.length < 3)
                return null;
            String dbName = tags[0];
            String packName = tags[1];
            String funcName = tags[2];
            LOG.info("find procedure in package: " + packName);
            if (packageFunctions.containsKey(packName)) {
                LOG.info("procedure already in local package");
                LOG.info("procedure full name: " + name);
                return packageFunctions.get(packName).get(name);
            } else
                loadPackFromDb(tags[0]);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public CommonProcedureStatement findFunc(String varName) {
        String funcName = varName.toUpperCase();
        TreeNode[] blocks = session.getCurrentScopes();
        // procedure inside the scope
        for (TreeNode blk: blocks) {
            ConcurrentHashMap<String, List<CommonProcedureStatement>> blkProcs = newFunctions.get(blk);
            if (blkProcs != null && blkProcs.get(funcName) != null) {
                List<CommonProcedureStatement> procs = blkProcs.get(funcName);
                return (procs.size() >= 1) ? procs.get(0) : null;
            }
        }
        // global procedure
        TreeNode rootScope = session.getRootNode();
        if (rootScope != null) {
            ConcurrentHashMap<String, List<CommonProcedureStatement>> procMap = newFunctions.get(rootScope);
            if (procMap != null) {
                List<CommonProcedureStatement> procs = newFunctions.get(rootScope).get(funcName);
                if (procs.size() >= 1)
                    return procs.get(0);
//            return (procs.size() >= 1) ? procs.get(0) : null;
            }
        }
        // for procedure in package
        return findProcedureInPackage(funcName);
    }

    // check all procedure signature belong to current scope
    public CommonProcedureStatement findFunc(String procName, List<Var> paras) {
        if (session.getCurrentScope() == null)
            return findFunc(procName);
        // TODO get proc based on signature
        return findFunc(procName);
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
        this.localCursors = new ConcurrentHashMap<String, CommonCursor>();
    }

    public Var addOrUpdateSys(Var var) {
        var.setExecuted(true);
        return systemVariables.put(var.getVarName().toUpperCase(), var);
    }

    public boolean updateSys(String sysVarName, Object val) {
        Var v = systemVariables.get(sysVarName.toUpperCase());
        if (null == v) {
            return false;
        }
        v.setVarValue(val);
        v.setExecuted(true);
        return true;
    }

    public void addType(LocalTypeDeclare typeDeclare) {
        if (session.isPackageScope()) {
            LOG.info("variable container add type in package scope: " + session.getPackageName() + ", type: " + typeDeclare.getTypeName());
            if (!packageTypes.containsKey(session.getPackageName())) {
                ConcurrentHashMap<String, LocalTypeDeclare> tmpMap = new ConcurrentHashMap<>();
                packageTypes.put(session.getPackageName(), tmpMap);
            }
            packageTypes.get(session.getPackageName()).put(typeDeclare.getTypeName().toUpperCase(), typeDeclare);
            return;
        }
        TreeNode curBlock = session.getCurrentScope();
        ConcurrentHashMap<String, LocalTypeDeclare> typeMap = new ConcurrentHashMap<>();
        typeMap.put(typeDeclare.getTypeName().toUpperCase(), typeDeclare);
        if (curBlock != null) {
            if (types.containsKey(curBlock)) {
                types.get(curBlock).put(typeDeclare.getTypeName().toUpperCase(), typeDeclare);
            } else {
                types.put(curBlock, typeMap);
            }
        } else {
            if (types.containsKey(session.getRootNode())) {
                types.get(session.getRootNode()).put(typeDeclare.getTypeName().toUpperCase(), typeDeclare);
            } else {
                types.put(curBlock, typeMap);
            }
        }
    }

    public LocalTypeDeclare findType(String name) {
        String scopeName = "";
        if (name.contains(".")) {
            String[] fullVarArray = name.split("\\.");
            if (fullVarArray.length == 2) {
                scopeName = fullVarArray[0];
                name = fullVarArray[1];
            }
        }
        TreeNode[] blocks = session.getCurrentScopes();
        for (TreeNode blk: blocks) {
            if (!scopeName.isEmpty()) {
                BaseStatement stmt = (BaseStatement) blk;
                if (!stmt.existLabel(scopeName))
                    continue;
            }
            ConcurrentHashMap<String, LocalTypeDeclare> typeMap = types.get(blk);
            if (typeMap != null && typeMap.get(name.toUpperCase()) != null) {
                return typeMap.get(name.toUpperCase());
            } else {
                if (!scopeName.isEmpty())
                    return null;
            }
        }
        // x.y, x is the package name
        LocalTypeDeclare packageType = findTypeInPackage(scopeName.toUpperCase(), name.toUpperCase());
        return packageType;
    }

    private LocalTypeDeclare findTypeInPackage(String packageName, String typeName) {
        boolean loadSuccess = false;
        if (packageName.isEmpty()) {
            // for ref-type in package
            if (session.isPackageScope())
                packageName = session.getPackageName().toUpperCase();
        }
        if (!packageTypes.containsKey(packageName.toUpperCase())) {
            // types will not persist in sc for crossing session, only need store into db
            loadSuccess = loadPackFromDb(packageName.toUpperCase());
        }
        LOG.info("package types load status: " + loadSuccess);
        if (packageTypes.containsKey(packageName)) {
            LOG.info("package types already in local packageTypes");
            return packageTypes.get(packageName).get(typeName);
        } else {
            return null;
        }
    }

}
