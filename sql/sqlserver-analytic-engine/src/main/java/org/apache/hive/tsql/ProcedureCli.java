package org.apache.hive.tsql;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.lang.StringUtils;
import org.apache.hive.tsql.another.GoStatement;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.exception.ParserErrorListener;
import org.apache.hive.tsql.exception.ParserException;
import org.apache.hive.tsql.execute.Executor;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by zhongdg1 on 2017/1/6.
 */
public class ProcedureCli {
    private boolean isTsql = true;
    private ParseTree tree = null;
    private ExecSession session = null;
    private ParserErrorListener listener;
    private SparkSession sparkSession = null;
    private static final Logger LOG = LoggerFactory.getLogger(ProcedureCli.class);

//    public SparkSession getSparkSession() {
//        return sparkSession;
//    }

    public ProcedureCli(SparkSession ss) {
        listener = new ParserErrorListener();
//        session = ExecSession.getSession();
        session = new ExecSession();
        //session.setSparkSession(sparkSession);
        session.setSparkSession(ss);
        sparkSession = ss;
    }

    public ExecSession getExecSession() {
        return session;
    }


    public void callProcedure(String sql) throws Throwable {
        try {
            long startTime=System.currentTimeMillis();
            //1. parser sql to tree
            LOG.info("query sql is " + sql);
            parse(sql);
            long startTime_parse=System.currentTimeMillis();
            LOG.warn("parse sql use :"+(startTime_parse-startTime));
            if (!listener.getExceptions().isEmpty()) {
                printExceptions(listener.getExceptions());
                // return;
                throw new Exception(listener.getExceptions().get(0));
            }

            //2. visit tree
            TExec visitor = new TExec(session.getRootNode());
            if (isTsql) {
                session.setVisitor(visitor);
            } else {
                LOG.info("Not TSQL ....");
            }
            visitor.visit(tree);
            long visitTime=System.currentTimeMillis();
            LOG.warn("visitor sql use :"+(visitTime-startTime_parse));
            LOG.info("Visit Tree completed, waiting for executing....");
            if (visitor.getExceptions().size() > 0) {
                printExceptions(visitor.getExceptions());
                //return;
                throw new Exception(visitor.getExceptions().get(0));
            }

            //3. check treenode
            check();

            //4. optimize
            optimize();

            //5. execute treenode
            Executor executor = new Executor(session);
            executor.run();
            long executorTime=System.currentTimeMillis();
            LOG.warn("visitor sql use :"+(executorTime-visitTime));
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

    public void parse(String sql) throws ParserException {
        InputStream inputStream = null;
        try {
//            inputStream = new FileInputStream(sql);
            inputStream = new ByteArrayInputStream(sql.getBytes("UTF-8"));
            ANTLRInputStream input = new ANTLRInputStream(inputStream);

            TSqlLexer lexer = new TSqlLexer(input);
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            TSqlParser parser = new TSqlParser(tokens);

            parser.addErrorListener(listener);
            tree = parser.tsql_file();

        } catch (Exception e) {
            throw new ParserException("Parse SQL ERROR # " + e);
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void printExceptions(List<Exception> exceptions) {
        for (Exception exception : exceptions) {
            System.err.println(exception);
        }
    }

}
