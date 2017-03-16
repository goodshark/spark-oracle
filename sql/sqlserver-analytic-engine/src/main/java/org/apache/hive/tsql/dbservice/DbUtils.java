package org.apache.hive.tsql.dbservice;

import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by wangsm9 on 2017/1/19.
 */
public class DbUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DbUtils.class);

    private String dbUrl;
    private String userName;
    private String password;

    public DbUtils(String dbUrl, String userName, String password) {
        this.dbUrl = dbUrl;
        this.userName = userName;
        this.password = password;
    }

    public Connection getConn() {
        Properties props = new Properties();
        props.put("user", userName);
        props.put("password", password);
        Connection conn = JdbcUtils.createConnectionFactory(dbUrl, props).apply();
        return conn;
    }

}
