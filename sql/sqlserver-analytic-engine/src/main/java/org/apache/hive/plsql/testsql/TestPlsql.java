package org.apache.hive.plsql.testsql;

import org.apache.hive.tsql.ProcedureCli;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by wangsm9 on 2017/7/12.
 */
public class TestPlsql {
    private static final Logger LOG = LoggerFactory.getLogger(TestPlsql.class);

    public static void main(String[] args) throws Throwable {

        ProcedureCli procedureCli = new ProcedureCli(null);
        procedureCli.setEngineName("oracle");
        procedureCli.callProcedure(SelectSqlCase.SLECT_SQL_002);
    }
}
