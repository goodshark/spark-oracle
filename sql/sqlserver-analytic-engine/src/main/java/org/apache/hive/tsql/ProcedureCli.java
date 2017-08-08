package org.apache.hive.tsql;

import org.apache.commons.lang.StringUtils;
import org.apache.hive.basesql.Engine;
import org.apache.hive.basesql.EngineManager;
import org.apache.hive.tsql.another.GoStatement;
import org.apache.hive.tsql.exception.ParserErrorListener;
import org.apache.hive.tsql.execute.Executor;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Created by zhongdg1 on 2017/1/6.
 */
public class ProcedureCli {
    private ExecSession session = null;
    private ParserErrorListener listener;
    private SparkSession sparkSession = null;
    private static final Logger LOG = LoggerFactory.getLogger(ProcedureCli.class);

    static {
        // load all supported sql engine
        try {
            Class.forName("org.apache.hive.tsql.SqlserverEngine");
            Class.forName("org.apache.hive.plsql.OracleEngine");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public ProcedureCli(SparkSession ss) {
        listener = new ParserErrorListener();
        session = new ExecSession();
        session.setSparkSession(ss);
        sparkSession = ss;
    }

    public ExecSession getExecSession() {
        return session;
    }


    public void callProcedure(String sql, String engineName) throws Throwable {
        try {
            session.setEngineName(engineName);
            LOG.info("spark-engine: " + engineName + ", query sql is " + sql);
            // generate execute plan
            buildExecutePlan(sql, engineName);
            // check treenode
            check();
            // optimize
            optimize();
            // execute treenode
            Executor executor = new Executor(session);
            executor.run();
            LOG.info("size is =======>" + session.getResultSets().size());
        } catch (Throwable e) {
            LOG.error("callProcedure error, sql:" + sql, e);
            throw e;
        } finally {
            //6. clean
            clean();
        }
    }

    private void optimize() {
    }

    private void check() {

    }


    /*public Set<String> getTempTables() {
//        return session.getVariableContainer().getAllTmpTableNames();
        Set<String> tables = new HashSet<>();
        for (String name : session.getVariableContainer().getAllTmpTableNames()) {
            String alias = session.getVariableContainer().findTmpTaleAlias(name);
            if (StringUtils.isBlank(alias)) {
                continue;
            }
            tables.add(alias);
        }
        return tables;
    }*/

    private void clean() {
        GoStatement.clearGoSeq();
        //清理表变量
        if (null != session) {
            HashMap<String, String> allAlias = new HashMap<>();
            for (String name : session.getVariableContainer().getAllTableVarNames()) {
                String alias = session.getVariableContainer().findTableVarAlias(name);
                if (StringUtils.isBlank(alias)) {
                    continue;
                }
                allAlias.put(name, alias);
            }
            HashMap<String, String> table = sparkSession.getSqlServerTable().get(1);
            if (null != table) {
                sparkSession.getSqlServerTable().get(1).putAll(allAlias);
            } else {
                sparkSession.getSqlServerTable().put(1, allAlias);
            }

            /* for (String name : session.getVariableContainer().getAllTmpTableNames()) {
                String alias = session.getVariableContainer().findTmpTaleAlias(name);
                if (StringUtils.isBlank(alias)) {
                    continue;
                }
                allAlias.add(alias);
            } */

            /*for (String tableName : allAlias) {
                final StringBuffer sb = new StringBuffer();
                sb.append("DROP TABLE ").append(tableName);
                BaseStatement statement = new BaseStatement() {
                    @Override
                    public BaseStatement createStatement() {
                        return null;
                    }

                    @Override
                    public int execute() {
                        commitStatement(sb.toString());
                        return 0;
                    }
                };
                statement.setExecSession(session);
                try {
                    statement.execute();
                } catch (Throwable e) {
                    LOG.error("clean error", e);
                }

            }*/
        }
    }

    private void buildExecutePlan(String sql, String engineName) throws Throwable {
        Engine engine = EngineManager.getEngine(engineName);
        engine.setSession(session);
        engine.parse(sql);
        engine.visitTree();
    }
}
