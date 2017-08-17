package org.apache.hive.tsql.dbservice;

import com.google.gson.Gson;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.catalyst.plfunc.PlFunctionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.util.parsing.json.JSON;
import scala.util.parsing.json.JSONObject;

import java.io.ObjectOutputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by chenfolin on 2017/8/16.
 */
public class PlFunctionService {

    private static final Logger logger = LoggerFactory.getLogger(PlFunctionService.class);

    public static final int ORACLE_FUNCTION_TYPE = 3;

    private static PlFunctionService plFunctionService;
    private static AtomicBoolean singleLetonDone = new AtomicBoolean(false);

    private final static String TABLE_NAME = "LENOVO_SQLSERVER_PROCEDURE";
    private final static int TYPE = 3;
    private final static String ENGINE_NAME = "spark.sql.analytical.engine";

    private String dbUrl;
    private String userName;
    private String password;
    private PlFunctionService(String dbUrl, String userName, String password){
        this.dbUrl = dbUrl;
        this.userName = userName;
        this.password = password;
    }

    public static PlFunctionService getInstance(String dbUrl, String userName, String password){
        if(singleLetonDone.get() == false){
            synchronized (singleLetonDone) {
                if(plFunctionService == null){
                    plFunctionService = new PlFunctionService(dbUrl, userName, password);
                    singleLetonDone.set(true);
                }
            }
        }
        return plFunctionService;
    }

    public int createPlFunction(PlFunctionRegistry.PlFunctionDescription function, String userName, int type) throws Exception{
        int rs = 0;
        String funcName = function.getFunc().toString();
        String funcSql = function.getBody();
        String md5InDb = function.getMd5();
        StringBuffer sql = new StringBuffer();
        sql.append("  INSERT INTO ").append(TABLE_NAME);
        sql.append("(");
        sql.append("PROC_NAME,")
                .append("PROC_CONTENT,")
                .append("PROC_OBJECT,")
                .append("CREATE_TIME,")
                .append("MD5,")
                .append("DB_NAME,")
                .append("USE_NAME,")
                .append("TYPE,")
                .append("PROC_ORC_NAME");
        sql.append(") ");
        sql.append(" VALUES");
        sql.append(" (");
        sql.append(" ?,?,?,?,?,?,?,?,?");
        sql.append(" )");
        Connection connection = null;
        PreparedStatement stmt = null;

        try {
            DbUtils dbUtils = new DbUtils(dbUrl, userName, password);
            connection = dbUtils.getConn();
            stmt = connection.prepareStatement(sql.toString());
            stmt.setString(1, funcName);
            stmt.setString(2, funcSql);

            Gson gson = new Gson();
            String data = gson.toJson(function);
            stmt.setBytes(3, data.getBytes("UTF-8"));
            stmt.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
            stmt.setString(5, function.getMd5());


            stmt.setString(6, function.getFunc().getDb());
            stmt.setString(7, userName);
            stmt.setInt(8, type);
            stmt.setString(9, funcName);
            rs = stmt.executeUpdate();
        } catch (SQLException e) {
            logger.error(" execute create pl function sql : " + sql.toString() + " error.", e);
            throw e;
        } finally {
            close(connection, stmt);
        }
        return rs;
    };

    public int updatePlFunction(PlFunctionRegistry.PlFunctionDescription function, String userName, int type) throws Exception {
        StringBuffer sql = new StringBuffer();
        sql.append("  UPDATE ").append(TABLE_NAME);
        sql.append(" SET  ");
        sql.append(" PROC_CONTENT = ?,");
        sql.append(" PROC_OBJECT = ?,");
        sql.append(" MD5 = ?,");
        sql.append(" DB_NAME = ?,");
        sql.append(" USE_NAME = ?,");
        sql.append(" UPDATE_TIME = ?,");
        sql.append(" PROC_ORC_NAME = ?");
        sql.append(" WHERE ");
        sql.append("PROC_NAME =");
        sql.append("?");
        sql.append(" AND  DEL_FLAG= 1");
        sql.append(" AND TYPE = " + type);
        Connection connection = null;
        PreparedStatement stmt = null;
        int rs = 0;
        try {
            DbUtils dbUtils = new DbUtils(dbUrl, userName, password);
            connection = dbUtils.getConn();
            stmt = connection.prepareStatement(sql.toString());
            stmt.setString(1, function.getBody());
            Gson gson = new Gson();
            String data = gson.toJson(function);
            stmt.setBytes(2, data.getBytes("UTF-8"));
            stmt.setString(3, function.getMd5());
            stmt.setString(4, function.getFunc().getDb());
            stmt.setString(5, userName);
            stmt.setTimestamp(6, new Timestamp(System.currentTimeMillis()));
            stmt.setString(7, function.getFunc().toString());
            stmt.setString(8, function.getFunc().toString());
            rs = stmt.executeUpdate();
        } catch (SQLException e) {
            logger.error(" update plfunction sql : " + sql.toString() + " error.", e);
            throw e;
        } finally {
            close(connection, stmt);
        }
        return rs;
    }

    public List<PlFunctionRegistry.PlFunctionDescription> getPlFunctions(int type) throws Exception {
        StringBuffer sql = new StringBuffer();
        sql.append("  SELECT *  FROM ").append(TABLE_NAME);
        sql.append(" WHERE ");
        sql.append(" DEL_FLAG =1");
        sql.append(" AND TYPE = " + type);
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet rs;
        List<PlFunctionRegistry.PlFunctionDescription> list = new ArrayList<>();
        try {
            DbUtils dbUtils = new DbUtils(dbUrl, userName, password);
            connection = dbUtils.getConn();
            stmt = connection.prepareStatement(sql.toString());
            rs = stmt.executeQuery();
            Gson gson = new Gson();
            while (rs.next()) {
                byte[] data = rs.getBytes("PROC_OBJECT");
                list.add(gson.fromJson(new String(data, "UTF-8"), PlFunctionRegistry.PlFunctionDescription.class));
            }
        } catch (Exception e) {
            logger.error(" execute getPlFunctions sql : " + sql.toString() + " error.", e);
            throw e;
        } finally {
            close(connection, stmt);
        }
        return list;
    }

    public int delPlFunction(PlFunctionRegistry.PlFunctionIdentify func, int type) throws SQLException {
        logger.info(" del function by funName:" + func);
        StringBuffer sql = new StringBuffer();
        sql.append("  UPDATE ").append(TABLE_NAME);
        sql.append(" SET DEL_FLAG =-1 ");
        sql.append(" WHERE ");
        sql.append("PROC_NAME =");
        sql.append("?");
        sql.append(" AND TYPE = " + type);
        Connection connection = null;
        PreparedStatement stmt = null;
        int rs = 0;
        try {
            DbUtils dbUtils = new DbUtils(dbUrl, userName, password);
            connection = dbUtils.getConn();
            stmt = connection.prepareStatement(sql.toString());
            stmt.setString(1, func.toString());
            rs = stmt.executeUpdate();
        } catch (SQLException e) {
            logger.error(" execute delete function sql : " + sql.toString() + " error.", e);
            throw e;
        } finally {
            close(connection, stmt);
        }
        return rs;
    }

    private void close(Connection connection, Statement stmt) throws SQLException {
        if (null != connection) {
            connection.close();
        }
        if (null != stmt) {
            stmt.close();
        }
    }

}
