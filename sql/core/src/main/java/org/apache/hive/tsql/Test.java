package hive.tsql;

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
        procedureCli.callProcedure("with t_bTemp(i,n,a)\n" +
                "as (select b1.id, b1.name, b2.age from b1 inner join b2 on b1.id = b2.id)\n" +
                "insert into b3 select i,n,a from t_bTemp");

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
}
