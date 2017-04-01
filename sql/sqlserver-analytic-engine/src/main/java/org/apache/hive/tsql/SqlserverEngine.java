package org.apache.hive.tsql;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.hive.basesql.Engine;
import org.apache.hive.basesql.EngineManager;
import org.apache.hive.tsql.exception.ParserErrorListener;
import org.apache.hive.tsql.exception.ParserException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by dengrb1 on 4/1 0001.
 */

public class SqlserverEngine implements Engine {
    private static final Logger LOG = LoggerFactory.getLogger(SqlserverEngine.class);
    private static final String _name = "sqlserver";
    private ParserErrorListener listener = new ParserErrorListener();
    private ParseTree tree;
    private ExecSession session = null;

    static {
        try {
            EngineManager.registerEngine(_name, new SqlserverEngine());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setSession(ExecSession es) {
        session = es;
    }

    @Override
    public void parse(String sql) throws Throwable {
        InputStream inputStream = null;
        try {
            inputStream = new ByteArrayInputStream(sql.getBytes("UTF-8"));
            ANTLRInputStream input = new ANTLRInputStream(inputStream);

            TSqlLexer lexer = new TSqlLexer(input);
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            TSqlParser parser = new TSqlParser(tokens);

            parser.addErrorListener(listener);
            tree = parser.tsql_file();

        } catch (Exception e) {
            throw new ParserException("Parse " + _name + " SQL ERROR # " + e);
        } finally {
            if (null != inputStream) {
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        if (!listener.getExceptions().isEmpty()) {
            throw new Exception(listener.getExceptions().get(0));
        }
    }

    @Override
    public void visitTree() throws Throwable {
        TExec visitor = new TExec(session.getRootNode());
        session.setVisitor(visitor);
        visitor.visit(tree);
        LOG.info("Visit " + _name + " Tree completed, waiting for executing....");
        if (visitor.getExceptions().size() > 0) {
            throw new Exception(visitor.getExceptions().get(0));
        }
    }

}
