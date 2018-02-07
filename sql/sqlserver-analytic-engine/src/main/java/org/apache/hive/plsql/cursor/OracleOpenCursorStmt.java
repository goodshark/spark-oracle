package org.apache.hive.plsql.cursor;

import org.apache.hive.basesql.cursor.CommonOpenCursorStmt;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.common.BaseStatement;
import org.apache.hive.tsql.common.SparkResultSet;
import org.apache.hive.tsql.dml.ExpressionStatement;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 7/17 0017.
 */
public class OracleOpenCursorStmt extends CommonOpenCursorStmt {
    private static final String STATEMENT_NAME = "_ORACLE_OPEN_CURSOR_";

    private List<ExpressionStatement> expressions = new ArrayList<>();

    public OracleOpenCursorStmt() {
    }

    public OracleOpenCursorStmt(String name, boolean global) {
        super(STATEMENT_NAME, name, global);
    }

    @Override
    public int preSqlExecute() throws Exception {
        // TODO add cursor scope into GLOBAL
        getExecSession().enterScope(cursor);
        try {
            // TODO bind vars with values
            List<Var> paras = bindParameters();
            // TODO add cursor parameters into GLOBAL {cursor-scope -> var}
            addParaIntoScope(paras);
            return 0;
        } catch (Exception e) {
            getExecSession().leaveScope();
            throw e;
        }
    }

    @Override
    public int postExecute() throws Exception {
        storeSchema();
        // TODO pop cursor scope from GLOBAL-SCOPE
        getExecSession().leaveScope();
        // TODO the cursor-parameter will stay in VariableContainer, delete cursor-parameters from GLOBAL-VARS safely
        return 0;
    }

    private void storeSchema() {
        SparkResultSet rs = (SparkResultSet) cursor.getRs();
        ((OracleCursor)cursor).setSchema(rs.getColumns());
    }

    private List<Var> bindParameters() throws Exception {
        if (((OracleCursor)cursor).getParameterSize() != expressions.size())
            throw new Exception("parameter-cursor parameter counts is NOT match");
        List<Var> paras = ((OracleCursor)cursor).getCursorParaCopys();
        for (ExpressionStatement es: expressions) {
            es.setExecSession(getExecSession());
            es.execute();
        }
        for (int i = 0; i < expressions.size(); i++) {
            paras.get(i).setVarValue(expressions.get(i).getRs().getObject(0));
        }
        return paras;
    }

    private void addParaIntoScope(List<Var> vars) throws Exception {
        for (Var v: vars) {
            getExecSession().getVariableContainer().addVar(v);
        }
    }

    public void addArg(ExpressionStatement es) {
        expressions.add(es);
    }

    @Override
    public BaseStatement createStatement() {
        return null;
    }
}
