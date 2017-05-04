package org.apache.hive.tsql.udf;

import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.common.TmpTableNameUtils;
import org.apache.hive.tsql.dbservice.DbUtils;
import org.apache.hive.tsql.dbservice.ProcService;
import org.apache.hive.tsql.exception.FunctionArgumentException;
import org.apache.hive.tsql.util.StrUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

/**
 * Created by dengrb1 on 2/14 0014.
 */
public class ObjectIdCalculator extends BaseCalculator {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectIdCalculator.class);
    private final String OBJ_TABLE_NAME = "TBLS";
    private final String DB_TABLE_NAME = "DBS";
    private final String TEMP_TBL_DB = "tmp";

    public ObjectIdCalculator() {
    }

    /**
     * AF = 聚合函数 (CLR)
     * C = CHECK 约束
     * D = DEFAULT（约束或独立）
     * F = FOREIGN KEY 约束
     * FN = SQL 标量函数
     * FS = 程序集 (CLR) 标量函数
     * FT = 程序集 (CLR) 表值函数
     * IF = SQL 内联表值函数
     * IT = 内部表
     * P = SQL 存储过程
     * PC = 程序集 (CLR) 存储过程
     * PG = 计划指南
     * PK = PRIMARY KEY 约束
     * R = 规则（旧式，独立）
     * RF = 复制筛选过程
     * S = 系统基表
     * SN = 同义词
     * SO = 序列对象
     *
     * from 2012 sql-server
     * SQ = 服务队列
     * TA = 程序集 (CLR) DML 触发器
     * TF = SQL 表值函数
     * TR = SQL DML 触发器
     * TT = 表类型
     * U = 表（用户定义类型）
     * UQ = UNIQUE 约束
     * V = 视图
     * X = 扩展存储过程
     * ET = 外部表
     */
    @Override
    public Var compute() throws Exception {
        Var resVar = new Var("objectID", null, Var.DataType.NULL);
        boolean res = false;

        List<Var> argList = getAllArguments();
        if (argList.size() < 1 || argList.size() > 2)
            throw new FunctionArgumentException("OBJECT_ID", argList.size(), 1, 2);

        String objName = getArguments(0).getVarValue().toString();
        objName = StrUtils.trimQuot(objName);
        if (argList.size() == 1) {
            LOG.info("ObjectId check all object");
            res = memoryFind(objName);
            if (!res)
                res = databaseFind(objName, "");
        } else {
            String arg = getArguments(1).getVarValue().toString();
            arg = StrUtils.trimQuot(arg).toUpperCase();
            switch (arg) {
                case "AF":
                case "FN":
                    // function in memory
                    LOG.info("ObjectId check in memory");
                    res = memoryFind(objName);
                    break;
                case "IT":
                case "ET":
                case "P":
                case "U":
                case "V":
                    // object in database
                    LOG.info("ObjectId check in database");
                    res = databaseFind(objName, arg);
                    break;
                default:
                    break;
            }
        }

        if (res) {
            resVar.setDataType(Var.DataType.INT);
            resVar.setVarValue(0);
        }
        return resVar;
    }

    private boolean memoryFind(String funcName) {
        return UdfFactory.funcExists(funcName.toUpperCase());
    }

    public boolean databaseFind(String objName, String type) throws Exception {
        if (type.equalsIgnoreCase("P")) {
            // procedure only
            return procedureCheck(objName);
        } else if (!type.isEmpty()){
            // other object
            return objCheck(objName, type);
        }
        // all object
        if (type.isEmpty()) {
            LOG.info("ObjectId check all object, type is empty");
            return procedureCheck(objName) || objCheck(objName, type);
        }
        return false;
    }

    /**
     * transform full table name: server.database.schema.table -> database.table
     * transform full proc name: database.schema.proc -> database.proc
     */
    private String getSimpleObjName(String fullName, String type) throws Exception {
        if (fullName.isEmpty())
            return "";
        String[] nameArray = fullName.split("\\.");
        String objName = nameArray[nameArray.length-1];
        String dbName = "";
        if (nameArray.length >= 3) {
            dbName = nameArray[nameArray.length-3];
        } else {
            dbName = getExecSession().getSparkSession().sessionState().catalog().getCurrentDatabase();
        }
        // all tmp table store in tmp DB
        if (objName.startsWith("#") || objName.startsWith("##")) {
            dbName = TEMP_TBL_DB;
            objName = getExecSession().getRealTableName(objName);
        }
        return dbName + "." + objName;
    }

    private boolean procedureCheck(String procName) throws Exception {
        ProcService dbConn = createProcDbConn();
        String simpleProcName = getSimpleObjName(procName, "P");
        LOG.info("ObjectId check procedure, simple name: " + simpleProcName);
        int cnt = dbConn.getCountByName(simpleProcName);
        return cnt >= 1 ? true : false;
    }

    private boolean objCheck(String objName, String type) throws Exception {
        LOG.info("ObjectId check obj in database, type: " + type);
        // generate sql
        String sqlStr = genSql(type);
        String curDb = "";
        String obj = "";
        String simpleTableName = getSimpleObjName(objName, type);
        if (!simpleTableName.isEmpty()) {
            String[] nameArray = simpleTableName.split("\\.");
            curDb = nameArray[0];
            obj = nameArray[1];
        }
        LOG.info("ObjectId check obj(not proc), simple name: " + simpleTableName + ", db: " + curDb + ", obj: " + obj);
        LOG.info("ObjectId check obj in database, sql: " + sqlStr + ", table: " + obj.toLowerCase() + ", db: " + curDb.toLowerCase());

        Connection conn = createDbConn();
        PreparedStatement stmt = conn.prepareStatement(sqlStr);
        stmt.setString(1, obj.toLowerCase());
        stmt.setString(2, curDb.toLowerCase());
        ResultSet rs = stmt.executeQuery();
        int count = 0;
        while (rs.next()) {
            count = rs.getInt(1);
        }
        return count >= 1 ? true : false;
    }

    private String genSql(String type) {
        StringBuffer sql = new StringBuffer();
        sql.append("select count(*) from ").append(OBJ_TABLE_NAME).append(" join ").append(DB_TABLE_NAME);
        sql.append(" on ").append(OBJ_TABLE_NAME).append(".DB_ID = ").append(DB_TABLE_NAME).append(".DB_ID");
        sql.append(" where ").append(OBJ_TABLE_NAME).append(".TBL_NAME = ");
        sql.append("?");
        sql.append(" and ").append(DB_TABLE_NAME).append(".NAME = ");
        sql.append("?");
        switch (type.toUpperCase()) {
            case "V":
                sql.append(" and TBL_TYPE = 'VIRTUAL_VIEW'");
                break;
            case "IT":
            case "U":
                sql.append(" and TBL_TYPE = 'MANAGED_TABLE'");
                break;
            case "ET":
                sql.append(" and TBL_TYPE = 'EXTERNAL_TABLE'");
                break;
            default:
                break;
        }
        return sql.toString();
    }

    private ProcService createProcDbConn() {
        return new ProcService(getExecSession().getSparkSession());
    }

    private Connection createDbConn() {
        String dbUrl = getExecSession().getSparkSession().sparkContext().hadoopConfiguration().get(Common.DBURL);
        String userName = getExecSession().getSparkSession().sparkContext().hadoopConfiguration().get(Common.USER_NAME);
        String password = getExecSession().getSparkSession().sparkContext().hadoopConfiguration().get(Common.PASSWORD);
        DbUtils dbUtils = new DbUtils(dbUrl, userName, password);
        Connection connection = dbUtils.getConn();
        return connection;
    }
}
