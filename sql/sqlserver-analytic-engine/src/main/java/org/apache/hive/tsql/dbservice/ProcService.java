package org.apache.hive.tsql.dbservice;


import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.func.Procedure;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.*;
import java.util.Date;

/**
 * Created by wangsm9 on 2017/1/19.
 */
public class ProcService {

    private static final Logger LOG = LoggerFactory.getLogger(ProcService.class);
    private final static String TABLE_NAME = "LENOVO_SQLSERVER_PROCEDURE";

    private SparkSession sparkSession;
    private String dbUrl;
    private String userName;
    private String password;


    public ProcService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        initDb();
    }

    public void initDb() {
        dbUrl = sparkSession.sparkContext().hadoopConfiguration().get(Common.DBURL);
        userName = sparkSession.sparkContext().hadoopConfiguration().get(Common.USER_NAME);
        password = sparkSession.sparkContext().hadoopConfiguration().get(Common.PASSWORD);
    }

    public int createProc(Procedure procedure) throws Exception {
        String procName = procedure.getName().getFullFuncName();
        int count = getCountByName(procName);
        LOG.info("count is ===>" + count);
        if (count > 0) {
            throw new SQLException(procName + " is exist;");
        }
        StringBuffer sql = new StringBuffer();
        sql.append("  INSERT INTO ").append(TABLE_NAME);
        sql.append("(");
        sql.append("PROC_NAME,")
                .append("PROC_CONTENT,")
                .append("PROC_OBJECT,")
                .append("CREATE_TIME,")
                .append("MD5");
        sql.append(") ");
        sql.append(" VALUES");
        sql.append(" (");
        sql.append(" ?,?,?,?,?");
        sql.append(" )");
        Connection connection = null;
        PreparedStatement stmt = null;
        ObjectOutputStream out = null;
        int rs = 0;
        try {
            DbUtils dbUtils = new DbUtils(dbUrl, userName, password);
            connection = dbUtils.getConn();
            stmt = connection.prepareStatement(sql.toString());
            stmt.setString(1, procedure.getName().getFullFuncName());
            stmt.setString(2, procedure.getProcSql());

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            out = new ObjectOutputStream(baos);
            out.writeObject(procedure);
            stmt.setBytes(3, baos.toByteArray());
            stmt.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
            stmt.setString(5, procedure.getMd5());
            rs = stmt.executeUpdate();
        } catch (SQLException e) {
            LOG.error(" execute createProc sql : " + sql.toString() + " error.", e);
            throw e;
        } finally {
            close(connection, stmt);
            out.close();
        }
        return rs;
    }


    public int delProc(String procName) throws SQLException {
        StringBuffer sql = new StringBuffer();
        sql.append("  DELETE FROM ").append(TABLE_NAME);
        sql.append(" WHERE ");
        sql.append("PROC_NAME =");
        sql.append("?");
        Connection connection = null;
        PreparedStatement stmt = null;
        int rs = 0;
        try {
            DbUtils dbUtils = new DbUtils(dbUrl, userName, password);
            connection = dbUtils.getConn();
            stmt = connection.prepareStatement(sql.toString());
            stmt.setString(1, procName);
            rs = stmt.executeUpdate();
        } catch (SQLException e) {
            LOG.error(" execute delProc sql : " + sql.toString() + " error.", e);
            throw e;
        } finally {
            close(connection, stmt);
        }
        return rs;
    }

    public int getCountByName(String procName) throws SQLException {
        StringBuffer sql = new StringBuffer();
        sql.append("  SELECT COUNT(1)  FROM ").append(TABLE_NAME);
        sql.append(" WHERE ");
        sql.append("PROC_NAME =");
        sql.append("?");
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet rs;
        int count = 0;
        try {
            DbUtils dbUtils = new DbUtils(dbUrl, userName, password);
            connection = dbUtils.getConn();
            stmt = connection.prepareStatement(sql.toString());
            stmt.setString(1, procName);
            rs = stmt.executeQuery();
            while (rs.next()) {
                count = rs.getInt(1);
            }
        } catch (SQLException e) {
            LOG.error(" execute getCountByName sql : " + sql.toString() + " error.", e);
            throw e;
        } finally {
            close(connection, stmt);
        }
        LOG.info(" execute getCountByName sql : " + sql.toString() + " count is ." + count);
        return count;
    }

    private void close(Connection connection, Statement stmt) throws SQLException {
        if (null != connection) {
            connection.close();
        }
        if (null != stmt) {
            stmt.close();
        }
    }

    public Procedure getProcContent(String procName) throws Exception {
        StringBuffer sql = new StringBuffer();
        sql.append("  SELECT *  FROM ").append(TABLE_NAME);
        sql.append(" WHERE ");
        sql.append("PROC_NAME =");
        sql.append("?");
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet rs;
        Procedure procedure = null;
        ObjectInputStream in = null;
        try {
            DbUtils dbUtils = new DbUtils(dbUrl, userName, password);
            connection = dbUtils.getConn();
            stmt = connection.prepareStatement(sql.toString());
            stmt.setString(1, procName);
            rs = stmt.executeQuery();
            if (rs.next()) {
                byte[] procedureObject = rs.getBytes("PROC_OBJECT");
                ByteArrayInputStream bais = new ByteArrayInputStream(procedureObject);
                in = new ObjectInputStream(bais);
                procedure = (Procedure) in.readObject();//从流中读取对象
            }
        } catch (Exception e) {
            LOG.error(" execute getCountByName sql : " + sql.toString() + " error.", e);
            throw e;
        } finally {
            close(connection, stmt);
            in.close();
        }
        return procedure;
    }


}
