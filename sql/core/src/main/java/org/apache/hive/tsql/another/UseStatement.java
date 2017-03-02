package org.apache.hive.tsql.another;

import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.LeafStatement;

import java.sql.ResultSet;

/**
 * Created by zhongdg1 on 2016/11/29.
 */
public class UseStatement extends LeafStatement {
    private final static String DEFAULT_DB_NAME = "default";
    private String dbName = DEFAULT_DB_NAME; //TODO: will store in arguments later

    private static final String STATEMENT_NAME = "_USE_";

    public UseStatement(String sql) {
        super(STATEMENT_NAME, sql);
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }


    // TODO: 2016/12/20  exceptions
    @Override
    public int execute() throws RuntimeException {
        ResultSet rs = commitStatement(super.getSql());
        return 0;
    }

    @Override
    public BaseStatement createStatement() {
        return this;
    }
}
