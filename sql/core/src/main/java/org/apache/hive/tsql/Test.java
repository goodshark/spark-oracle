package org.apache.hive.tsql;

import org.apache.hive.tsql.common.Row;
import org.apache.hive.tsql.common.SparkResultSet;

import static org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Method.get;

/**
 * Created by wangsm9 on 2017/2/8.
 */
public class Test {
    public static final  String CREATE_TABLE_SQL="create table t_BIGINT(n BIGINT)@\n" +
            "create table t_BINARY(n BINARY(50))@\n" +
            "create table t_BIT(n BIT)@\n" +
            "create table t_CHAR(n CHAR(50))@\n" +
            "create table t_DATE(n DATE)@\n" +
            "create table t_DATETIME(n DATETIME)@\n" +
            "create table t_DATETIME2(n DATETIME2)@\n" +
            "create table t_DATETIMEOFFSET(n DATETIMEOFFSET(50))@\n" +
            "create table t_DECIMAL(n DECIMAL(10, 10))@\n" +
            "create table t_FLOAT(n FLOAT)@\n" +
            "create table t_GEOGRAPHY(n GEOGRAPHY)@\n" +
            "create table t_GEOMETRY(n GEOMETRY)@\n" +
            "create table t_HIERARCHYID(n HIERARCHYID)@\n" +
            "create table t_IMAGE(n IMAGE)@\n" +
            "create table t_INT(n INT)@\n" +
            "create table t_MONEY(n MONEY)@\n" +
            "create table t_NCHAR(n NCHAR(50))@\n" +
            "create table t_NTEXT(n NTEXT)@\n" +
            "create table t_NUMERIC(n NUMERIC(10, 10))@\n" +
            "create table t_NVARCHAR(n NVARCHAR(50))@\n" +
            "create table t_NVARCHAR_MAX(n NVARCHAR(MAX))@\n" +
            "create table t_REAL(n REAL)@\n" +
            "create table t_SMALLDATETIME(n SMALLDATETIME)@\n" +
            "create table t_SMALLINT(n SMALLINT)@\n" +
            "create table t_SMALLMONEY(n SMALLMONEY)@\n" +
            "create table t_SQL_VARIANT(n SQL_VARIANT)@\n" +
            "create table t_TEXT(n TEXT)@\n" +
            "create table t_TIME(n TIME(50))@\n" +
            "create table t_TIMESTAMP(n TIMESTAMP)@\n" +
            "create table t_TINYINT(n TINYINT)@\n" +
            "create table t_UNIQUEIDENTIFIER(n UNIQUEIDENTIFIER)@\n" +
            "create table t_VARBINARY(n VARBINARY(50))@\n" +
            "create table t_VARBINARY_MAX(n VARBINARY(MAX))@\n" +
            "create table t_VARCHAR(n VARCHAR(50))@\n" +
            "create table t_VARCHAR_MAX(n VARCHAR(MAX))@\n" +
            "create table t_XML(n XML)@";

    public static void main(String[] args) throws Throwable {
       ProcedureCli procedureCli = new ProcedureCli(null);
        //procedureCli.callProcedure("insert into test001 values(1),(2)");
        procedureCli.callProcedure("exec p_func1 N'test_a', 110'");

        /*for (String sql:CREATE_TABLE_SQL.split("@")) {
            try{
                ProcedureCli procedureCli = new ProcedureCli(null);
                System.out.println(sql);
                procedureCli.callProcedure(sql);
            }catch (Throwable e){
                System.out.println(e);
            }

        }*/
    }

    public static  class testHashCode{
        private String s;
    }


    public static void replace(){
        String sql="select a as aa ,b as bb from tb";



    }
}