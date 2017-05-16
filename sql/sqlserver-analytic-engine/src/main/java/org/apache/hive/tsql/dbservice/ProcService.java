package org.apache.hive.tsql.dbservice;


import org.apache.commons.lang.StringUtils;
import org.apache.hive.tsql.ProcedureCli;
import org.apache.hive.tsql.common.Common;
import org.apache.hive.tsql.func.Procedure;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.*;

/**
 * Created by wangsm9 on 2017/1/19.
 */
public class ProcService {

    private static final Logger LOG = LoggerFactory.getLogger(ProcService.class);
    private final static String TABLE_NAME = "LENOVO_SQLSERVER_PROCEDURE";
    private final static int PRO_TYPE = 1;

    private SparkSession sparkSession;
    private String dbUrl;
    private String userName;
    private String password;
    private ProcedureCli procedureCli;


    public ProcService(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
        initDb();
    }

    public void initDb() {
        dbUrl = sparkSession.sparkContext().hadoopConfiguration().get(Common.DBURL);
        userName = sparkSession.sparkContext().hadoopConfiguration().get(Common.USER_NAME);
        password = sparkSession.sparkContext().hadoopConfiguration().get(Common.PASSWORD);
        procedureCli = new ProcedureCli(sparkSession);
    }

    public int createProc(Procedure procedure) throws Exception {
        int rs = 0;
        String procName = procedure.getName().getRealFullFuncName();
        ProcBean procBean = getProcBean(procName);
        String procSql = procBean.getProcContent();
        String md5InDb = procBean.getMd5();
        if (StringUtils.isNotBlank(md5InDb)) {
            if (StringUtils.isBlank(procSql)) {
                LOG.info("will update proc.............");
                updateProcObject(procedure);
            } else {
                throw new SQLException(procName + " is exist;");
            }
        } else {
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
            ObjectOutputStream out = null;

            try {
                DbUtils dbUtils = new DbUtils(dbUrl, userName, password);
                connection = dbUtils.getConn();
                stmt = connection.prepareStatement(sql.toString());
                stmt.setString(1, procName);
                stmt.setString(2, procedure.getProcSql());

                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                out = new ObjectOutputStream(baos);
                out.writeObject(procedure);
                stmt.setBytes(3, baos.toByteArray());
                stmt.setTimestamp(4, new Timestamp(System.currentTimeMillis()));
                stmt.setString(5, procedure.getMd5());


                stmt.setString(6, procedure.getName().getDatabase());
                stmt.setString(7, sparkSession.sparkSessionUserName());
                stmt.setInt(8, PRO_TYPE);
                stmt.setString(9, procedure.getName().getFullFuncName());

                rs = stmt.executeUpdate();
            } catch (SQLException e) {
                LOG.error(" execute createProc sql : " + sql.toString() + " error.", e);
                throw e;
            } finally {
                close(connection, stmt);
                out.close();
            }
        }
        return rs;
    }

    public int updateProc(Procedure procedure) throws Exception {
        StringBuffer sql = new StringBuffer();
        sql.append("  UPDATE ").append(TABLE_NAME);
        sql.append(" SET  ");
        sql.append(" PROC_CONTENT = ?,");
        sql.append(" PROC_OBJECT = ?,");
        sql.append(" MD5 = ?,");
        sql.append(" DB_NAME = ?,");
        sql.append(" USE_NAME = ?,");
        sql.append(" TYPE = ?,");
        sql.append(" UPDATE_TIME = ?,");
        sql.append(" PROC_ORC_NAME = ?");
        sql.append(" WHERE ");
        sql.append("PROC_NAME =");
        sql.append("?");
        sql.append(" AND  DEL_FLAG= 1");
        Connection connection = null;
        PreparedStatement stmt = null;
        int rs = 0;
        ObjectOutputStream out = null;
        try {
            DbUtils dbUtils = new DbUtils(dbUrl, userName, password);
            connection = dbUtils.getConn();
            stmt = connection.prepareStatement(sql.toString());
            stmt.setString(1, procedure.getProcSql());
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            out = new ObjectOutputStream(baos);
            out.writeObject(procedure);
            stmt.setBytes(2, baos.toByteArray());
            stmt.setString(3, procedure.getMd5());
            stmt.setString(4, procedure.getName().getDatabase());
            stmt.setString(5, sparkSession.sparkSessionUserName());
            stmt.setInt(6, PRO_TYPE);
            stmt.setTimestamp(7, new Timestamp(System.currentTimeMillis()));
            stmt.setString(8, procedure.getName().getFullFuncName());
            stmt.setString(9, procedure.getName().getRealFullFuncName());
            rs = stmt.executeUpdate();
        } catch (SQLException e) {
            LOG.error(" update Proc sql : " + sql.toString() + " error.", e);
            throw e;
        } finally {
            close(connection, stmt);
            out.close();
        }
        return rs;

    }


    public int updateProcObject(Procedure procedure) throws Exception {
        StringBuffer sql = new StringBuffer();
        sql.append("  UPDATE ").append(TABLE_NAME);
        sql.append(" SET  ");
        sql.append(" PROC_OBJECT = ?");
        sql.append(" ,PROC_CONTENT = ?");
        sql.append(" WHERE ");
        sql.append("PROC_NAME =");
        sql.append("?");
        sql.append(" AND  DEL_FLAG= 1");
        Connection connection = null;
        PreparedStatement stmt = null;
        int rs = 0;
        ObjectOutputStream out = null;
        try {
            DbUtils dbUtils = new DbUtils(dbUrl, userName, password);
            connection = dbUtils.getConn();
            stmt = connection.prepareStatement(sql.toString());
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            out = new ObjectOutputStream(baos);
            out.writeObject(procedure);
            stmt.setBytes(1, baos.toByteArray());
            stmt.setString(2, procedure.getProcSql());
            stmt.setString(3, procedure.getName().getRealFullFuncName());
            rs = stmt.executeUpdate();
        } catch (SQLException e) {
            LOG.error(" update Proc sql : " + sql.toString() + " error.", e);
            throw e;
        } finally {
            close(connection, stmt);
            out.close();
        }
        return rs;
    }


    public int delProc(String procName) throws SQLException {
        LOG.info(" del proc by procName:" + procName);
        StringBuffer sql = new StringBuffer();
        sql.append("  UPDATE ").append(TABLE_NAME);
        sql.append(" SET DEL_FLAG =-1 ");
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
        LOG.info(" get count  proc by procName:" + procName);
        StringBuffer sql = new StringBuffer();
        sql.append("  SELECT COUNT(1)  FROM ").append(TABLE_NAME);
        sql.append(" WHERE ");
        sql.append(" DEL_FLAG =1");
        sql.append(" AND ");
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

    public ProcBean getProcBean(String procName) throws Exception {
        StringBuffer sql = new StringBuffer();
        sql.append("  SELECT *  FROM ").append(TABLE_NAME);
        sql.append(" WHERE ");
        sql.append("PROC_NAME =");
        sql.append("?");
        sql.append(" AND DEL_FLAG =1");
        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet rs;
        ProcBean procBean = new ProcBean();
        try {
            DbUtils dbUtils = new DbUtils(dbUrl, userName, password);
            connection = dbUtils.getConn();
            stmt = connection.prepareStatement(sql.toString());
            stmt.setString(1, procName);
            rs = stmt.executeQuery();
            if (rs.next()) {
                String md5 = rs.getString("MD5");
                String procContent = rs.getString("PROC_CONTENT");
                String dbName = rs.getString("DB_NAME");
                String useName = rs.getString("USE_NAME");
                String procOrcName = rs.getString("PROC_ORC_NAME");
                procBean.setMd5(md5);
                procBean.setProcContent(procContent);
                procBean.setDbName(dbName);
                procBean.setUserName(useName);
                procBean.setProcOrcName(procOrcName);

            }
        } catch (Exception e) {
            LOG.error(" execute getProMd5 sql : " + sql.toString() + " error.", e);
            throw e;
        } finally {
            close(connection, stmt);

        }
        return procBean;
    }

    public Procedure getProcContent(String procName) throws Exception {
        StringBuffer sql = new StringBuffer();
        sql.append("  SELECT PROC_OBJECT, PROC_CONTENT FROM ").append(TABLE_NAME);
        sql.append(" WHERE ");
        sql.append("PROC_NAME =");
        sql.append("?");
        sql.append(" AND DEL_FLAG =1");
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
                try {
                    procedure = (Procedure) in.readObject();//从流中读取对象
                } catch (Exception ine) {
                    LOG.warn("SerialVersionUID is change, run proc again .");
                    try {
                        String sqlContent = rs.getString("PROC_CONTENT");
                        LOG.debug("query sql is " + sql.toString() + ",procName is " + procName + ",get sql is ==>" + sqlContent);
                        updateProSerialVersionUID(procName);
                        if (!StringUtils.isBlank(sqlContent)) {
                            procedureCli.callProcedure(sqlContent);
                            procedure = getProcContent(procName);
                        } else {
                            throw new Exception("procName:" + procName + ".the proc sql is null");
                        }
                    } catch (Throwable e) {
                        LOG.error("reRun proc again error .", e);
                        throw new Exception(e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(" execute getCountByName sql : " + sql.toString() + " error.", e);
            throw e;
        } finally {
            close(connection, stmt);
            if (null != in) {
                in.close();
            }
        }
        return procedure;
    }


    public int updateProSerialVersionUID(String procName) throws Exception {
        StringBuffer sql = new StringBuffer();
        sql.append("  UPDATE ").append(TABLE_NAME);
        sql.append(" SET  ");
        sql.append(" PROC_CONTENT = ?");
        sql.append(" WHERE ");
        sql.append("PROC_NAME =");
        sql.append("?");
        sql.append(" AND  DEL_FLAG= 1");
        Connection connection = null;
        PreparedStatement stmt = null;
        int rs = 0;
        try {
            DbUtils dbUtils = new DbUtils(dbUrl, userName, password);
            connection = dbUtils.getConn();
            stmt = connection.prepareStatement(sql.toString());

            stmt.setString(1, "");
            stmt.setString(2, procName);
            rs = stmt.executeUpdate();
        } catch (SQLException e) {
            LOG.error(" update ProSerialVersionUID sql : " + sql.toString() + " error.", e);
            throw e;
        } finally {
            close(connection, stmt);
        }
        return rs;
    }


}
