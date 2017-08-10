package org.apache.hive.plsql;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.hive.basesql.Engine;
import org.apache.hive.basesql.EngineManager;
import org.apache.hive.tsql.ExecSession;
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
public class OracleEngine implements Engine {
    private static final Logger LOG = LoggerFactory.getLogger(OracleEngine.class);
    private static final String _name = "oracle";
    private ParserErrorListener listener = new ParserErrorListener();
    private ParseTree tree = null;
    private ExecSession session = null;

    static {
        try {
            EngineManager.registerEngine(_name, new OracleEngine());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setSession(ExecSession ss) {
        session = ss;
    }

    @Override
    public void parse(String sql) throws Throwable {
        InputStream inputStream = null;
        try {
            inputStream = new ByteArrayInputStream(sql.getBytes("UTF-8"));
            ANTLRInputStream input = new ANTLRInputStream(inputStream);

            PlsqlLexer lexer = new PlsqlLexer(input);
            CommonTokenStream tokens = new CommonTokenStream(lexer);
            PlsqlParser parser = new PlsqlParser(tokens);

            parser.addErrorListener(listener);
            tree = parser.compilation_unit();
            if (!listener.getExceptions().isEmpty()) {
                throw new Exception(listener.getExceptions().get(0));
            }
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
            listener.getExceptions().clear();
        }
    }

    @Override
    public void visitTree() throws Throwable {
        PLsqlVisitorImpl pLsqlVisitor = new PLsqlVisitorImpl(session.getRootNode());
        session.setVisitor(pLsqlVisitor);
        pLsqlVisitor.setSparkSession(session.getSparkSession());
        pLsqlVisitor.visit(tree);
        LOG.info("Visit " + _name + " Tree completed, waiting for executing....");
        if (pLsqlVisitor.getExceptions().size() > 0) {
            throw new Exception(pLsqlVisitor.getExceptions().get(0));
        }
    }
}
