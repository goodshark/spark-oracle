package org.apache.hive.plsql;

import org.antlr.v4.runtime.misc.Interval;
import org.apache.hive.basesql.TreeBuilder;
import org.apache.hive.plsql.PlsqlBaseVisitor;
import org.apache.hive.tsql.cfl.BeginEndStatement;
import org.apache.hive.tsql.cfl.GotoStatement;
import org.apache.hive.tsql.common.TreeNode;
import scala.tools.nsc.backend.jvm.opt.BytecodeUtils;

import java.util.List;

/**
 * Created by dengrb1 on 3/31 0031.
 */

public class PlsqlVisitorImpl extends PlsqlBaseVisitor<Object> {
    private TreeBuilder treeBuilder = null;

    public PlsqlVisitorImpl(TreeNode rootNode) {
        treeBuilder = new TreeBuilder(rootNode);
    }

    public List<Exception> getExceptions() {
        return treeBuilder.getExceptions();
    }

    @Override
    public Object visitData_manipulation_language_statements(PlsqlParser.Data_manipulation_language_statementsContext ctx) {
        // TODO test only
        String sql = ctx.start.getInputStream().getText(
                new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
        return visitChildren(ctx);
    }

    @Override
    public Object visitCompilation_unit(PlsqlParser.Compilation_unitContext ctx) {
        for (PlsqlParser.Unit_statementContext unitCtx: ctx.unit_statement()) {
            visit(unitCtx);
            treeBuilder.addNode(treeBuilder.getRootNode());
        }
        for (PlsqlParser.Seq_of_statementsContext seqCtx: ctx.seq_of_statements()) {
            visit(seqCtx);
            treeBuilder.addNode(treeBuilder.getRootNode());
        }
        return null;
    }

    @Override
    public Object visitUnit_statement(PlsqlParser.Unit_statementContext ctx) {
        return visitChildren(ctx);
    }

    @Override
    public Object visitSeq_of_statements(PlsqlParser.Seq_of_statementsContext ctx) {
        BeginEndStatement beginEndStatement = new BeginEndStatement(TreeNode.Type.BEGINEND);
        for (PlsqlParser.Label_declarationContext labelCtx: ctx.label_declaration()) {
            visit(labelCtx);
            treeBuilder.addNode(beginEndStatement);
        }
        for (PlsqlParser.StatementContext stateCtx: ctx.statement()) {
            visit(stateCtx);
            treeBuilder.addNode(beginEndStatement);
        }
        treeBuilder.pushStatement(beginEndStatement);
        return beginEndStatement;
    }

    @Override
    public Object visitLabel_declaration(PlsqlParser.Label_declarationContext ctx) {
        GotoStatement gotoStatement = new GotoStatement(TreeNode.Type.GOTO);
        gotoStatement.setLabel(ctx.label_name().getText());
        treeBuilder.pushStatement(gotoStatement);
        return gotoStatement;
    }

    @Override
    public Object visitBlock(PlsqlParser.BlockContext ctx) {
        List<PlsqlParser.Declare_specContext> declareList =  ctx.declare_spec();
        for (PlsqlParser.Declare_specContext item: declareList) {
        }

        if (ctx.body() != null) {
        }
        return visitChildren(ctx);
    }
}
