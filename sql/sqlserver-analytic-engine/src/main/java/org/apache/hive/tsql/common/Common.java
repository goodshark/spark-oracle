package org.apache.hive.tsql.common;

/**
 * Created by wangsm9 on 2016/11/29.
 */
public class Common {
    public static final String SPACE = " ";

    public static final String SEMICOLON = ";";

    public static final String FROM = " from ";
    public static final String LIMIT = " limit ";
    public static final String NULL = "NULL";


    public static final String DATE_FUCTION_YEAR = "year";

    public static final String DATE_FUCTION_MONTH = "month";

    public static final String DATE_FUCTION_DAY = "day";
    public static final String DATE_FUCTION_HOUR = "hour";
    public static final String DATE_FUCTION_MIN = "minute";
    public static final String DATE_FUCTION_SECOND = "second";
    public static final String DATE_FUCTION_WEEKOFYEAR = "weekofyear";




    public static final String CREATE_DATA_BASE="createDatabase";
    public static final String DROP_DATA_BASE="dropDatabase";

    public static final String CREATE_INDEX="createIndex";
    public static final String DROP_INDEX="dropIndex";
    public static final String CREATE_STATISTICS="createStatistics";
    public static final String DROP_STATISTICS="dropStatistics";
    public static final String CREATE_TABLE="createTable";
    public static final String DROP_TABLE="dropTable";
    public static final String  CREATE_VIEW="createView";
    public static final String  DROP_VIEW="dropView";
    public static final String ALTER_TABLE="alterTable";
    public static final String  DROP_TYPE="dropType";
    public static final String  INSERT="insert";
    public static final String  UPDATE="update";
    public static final String  SELECT="select";
    public static final String  DELETE="delete";


    public static final String DBURL="javax.jdo.option.ConnectionURL";
    public static final String USER_NAME="javax.jdo.option.ConnectionUserName";
    public static final String PASSWORD="javax.jdo.option.ConnectionPassword";

    public static final String SHOW_TABLES ="showTables" ;


    public static final String CLUSTER_BY_COL_NAME="@_CLUSTER_BY_NAME_@";
    public static final String crudStr = "  CLUSTERED BY (" + CLUSTER_BY_COL_NAME +
            ") INTO 1 BUCKETS STORED AS ORC TBLPROPERTIES (\"transactional\"=\"true\")";
}
