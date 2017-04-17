package org.apache.hive.plsql;

import org.antlr.v4.runtime.misc.Interval;
import org.apache.commons.logging.Log;
import org.apache.hive.basesql.TreeBuilder;
import org.apache.hive.plsql.PlsqlBaseVisitor;
import org.apache.hive.plsql.block.AnonymousBlock;
import org.apache.hive.plsql.block.ExceptionHandler;
import org.apache.hive.plsql.function.Function;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.cfl.BeginEndStatement;
import org.apache.hive.tsql.cfl.GotoStatement;
import org.apache.hive.tsql.cfl.IfStatement;
import org.apache.hive.tsql.cfl.WhileStatement;
import org.apache.hive.tsql.common.ExpressionBean;
import org.apache.hive.tsql.common.TreeNode;
import org.apache.hive.tsql.dml.ExpressionStatement;
import org.apache.hive.tsql.node.LogicNode;
import org.apache.hive.tsql.node.PredicateNode;
import scala.tools.nsc.backend.jvm.opt.BytecodeUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by dengrb1 on 3/31 0031.
 */

public class PLsqlVisitorImpl extends PlsqlBaseVisitor<Object> {
    private TreeBuilder treeBuilder = null;

    public PLsqlVisitorImpl(TreeNode rootNode) {
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
        return null;
    }

    @Override
    public Object visitCompilation_unit(PlsqlParser.Compilation_unitContext ctx) {
        for (PlsqlParser.Unit_statementContext unitCtx: ctx.unit_statement()) {
            visit(unitCtx);
            treeBuilder.addNode(treeBuilder.getRootNode());
        }
        /*for (PlsqlParser.Seq_of_statementsContext seqCtx: ctx.seq_of_statements()) {
            visit(seqCtx);
            treeBuilder.addNode(treeBuilder.getRootNode());
        }*/
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
    public Object visitAnonymous_block(PlsqlParser.Anonymous_blockContext ctx) {
        AnonymousBlock anonymousBlock = new AnonymousBlock();
        for (PlsqlParser.Declare_specContext declareCtx: ctx.declare_spec()) {
            visit(declareCtx);
            treeBuilder.addNode(anonymousBlock);
        }
        if (ctx.body() != null) {
            visit(ctx.body());
            while (true) {
                TreeNode node = treeBuilder.popStatement();
                if (node == null)
                    break;
                else {
                    if (node instanceof ExceptionHandler)
                        anonymousBlock.addExecptionNode(node);
                    else
                        anonymousBlock.addNode(node);
                }
            }
        }
        treeBuilder.pushStatement(anonymousBlock);
        return anonymousBlock;
    }

    @Override
    public Object visitVariable_declaration(PlsqlParser.Variable_declarationContext ctx) {
        visit(ctx.variable_name());
        visit(ctx.type_spec());
        if (ctx.default_value_part() != null) {
        }
        return null;
    }

    /*@Override
    public Object visitBlock(PlsqlParser.BlockContext ctx) {
        List<PlsqlParser.Declare_specContext> declareList =  ctx.declare_spec();
        for (PlsqlParser.Declare_specContext item: declareList) {
        }

        if (ctx.body() != null) {
        }
        return visitChildren(ctx);
    }*/

    @Override
    public Object visitLogical_or_expression(PlsqlParser.Logical_or_expressionContext ctx) {
        LogicNode orNode = new LogicNode(TreeNode.Type.OR);
        List<PlsqlParser.Logical_and_expressionContext> andList = ctx.logical_and_expression();
        if (andList.size() == 1) {
            visit(andList.get(0));
            orNode = (LogicNode) treeBuilder.popStatement();
            treeBuilder.pushStatement(orNode);
            return orNode;
        } else {
            visit(andList.get(0));
            treeBuilder.addNode(orNode);
            visit(andList.get(1));
            treeBuilder.addNode(orNode);
            for (int i = 2; i < andList.size(); i++) {
                visit(andList.get(i));
                LogicNode tmpOrNode = new LogicNode(TreeNode.Type.OR);
                tmpOrNode.addNode(orNode);
                orNode = tmpOrNode;
                treeBuilder.addNode(orNode);
            }
        }
        treeBuilder.pushStatement(orNode);
        return orNode;
    }

    @Override
    public Object visitLogical_and_expression(PlsqlParser.Logical_and_expressionContext ctx) {
        LogicNode andNode = new LogicNode(TreeNode.Type.AND);
        List<PlsqlParser.Negated_expressionContext> negList = ctx.negated_expression();
        if (negList.size() == 1) {
            visit(negList.get(0));
            return null;
        } else {
            visit(negList.get(0));
            treeBuilder.addNode(andNode);
            visit(negList.get(1));
            treeBuilder.addNode(andNode);

            LogicNode tmpAndNode = new LogicNode(TreeNode.Type.AND);
            for (int i = 2; i < negList.size(); i++) {
                visit(negList.get(i));
                if (tmpAndNode.getChildrenNodes().size() >= 2) {
                    LogicNode newTmpAndNode = new LogicNode(TreeNode.Type.AND);
                    newTmpAndNode.addNode(andNode);
                    newTmpAndNode.addNode(tmpAndNode);
                    tmpAndNode = new LogicNode(TreeNode.Type.AND);
                    andNode = newTmpAndNode;
                    tmpAndNode.addNode(treeBuilder.popStatement());
                } else {
                    tmpAndNode.addNode(treeBuilder.popStatement());
                }
            }
            if (tmpAndNode.getChildrenNodes().size() == 1) {
                tmpAndNode.insertNode(0, andNode);
                andNode = tmpAndNode;
            } else if (tmpAndNode.getChildrenNodes().size() == 2) {
                LogicNode newTmpAndNode = new LogicNode(TreeNode.Type.AND);
                newTmpAndNode.addNode(andNode);
                newTmpAndNode.addNode(tmpAndNode);
                andNode = newTmpAndNode;
            }
            /*visit(negList.get(0));
            treeBuilder.addNode(andNode);
            visit(negList.get(1));
            treeBuilder.addNode(andNode);
            for (int i = 2; i < negList.size(); i++) {
                visit(negList.get(i));
                LogicNode tmpAndNode = new LogicNode(TreeNode.Type.AND);
                tmpAndNode.addNode(andNode);
                andNode = tmpAndNode;
                treeBuilder.addNode(andNode);
            }*/
        }
        treeBuilder.pushStatement(andNode);
        return andNode;
    }

    @Override
    public Object visitNegated_expression(PlsqlParser.Negated_expressionContext ctx) {
        LogicNode orNode = new LogicNode(TreeNode.Type.NOT);
        visit(ctx.equality_expression());
        treeBuilder.addNode(orNode);
        treeBuilder.pushStatement(orNode);
        return orNode;
    }

    @Override
    public Object visitRelational_expression(PlsqlParser.Relational_expressionContext ctx) {
        PredicateNode predicateNode = new PredicateNode(TreeNode.Type.PREDICATE);
        String op = ctx.relational_operator().getText();
        predicateNode.setOp(op);
        List<PlsqlParser.Compound_expressionContext> expressCtxList = ctx.compound_expression();
        if (expressCtxList.size() != 2)
            return null;
        for (PlsqlParser.Compound_expressionContext expressionContext: expressCtxList) {
            visit(expressionContext);
            treeBuilder.addNode(predicateNode);
        }
        treeBuilder.pushStatement(predicateNode);
        return predicateNode;
    }

    @Override
    public Object visitIf_statement(PlsqlParser.If_statementContext ctx) {
        IfStatement ifStatement = new IfStatement(TreeNode.Type.IF);
        IfStatement rootIfStatement = ifStatement;
        if (ctx.condition() != null) {
            visit(ctx.condition());
            ifStatement.setCondtion((LogicNode) treeBuilder.popStatement());
        }
        if (ctx.seq_of_statements() != null) {
            visit(ctx.seq_of_statements());
            treeBuilder.addNode(ifStatement);
        }
        for (PlsqlParser.Elsif_partContext elsifCtx: ctx.elsif_part()) {
            visit(elsifCtx);
            TreeNode subIfStatement = treeBuilder.popStatement();
            ifStatement.addNode(subIfStatement);
            ifStatement = (IfStatement) subIfStatement;
        }
        if (ctx.else_part() != null) {
            visit(ctx.else_part());
            treeBuilder.addNode(ifStatement);
        }
        treeBuilder.pushStatement(rootIfStatement);
        return rootIfStatement;
    }

    @Override
    public Object visitElsif_part(PlsqlParser.Elsif_partContext ctx) {
        IfStatement ifStatement = new IfStatement(TreeNode.Type.IF);
        if (ctx.condition() != null) {
            visit(ctx.condition());
            ifStatement.setCondtion((LogicNode) treeBuilder.popStatement());
        }
        if (ctx.seq_of_statements() != null) {
            visit(ctx.seq_of_statements());
            treeBuilder.addNode(ifStatement);
        }
        treeBuilder.pushStatement(ifStatement);
        return ifStatement;
    }

    /*@Override
    public Object visitElse_part(PlsqlParser.Else_partContext ctx) {
        if (ctx.seq_of_statements() != null) {
            return visit(ctx.seq_of_statements());
        }
        return null;
    }*/

    @Override
    public Object visitLoop_statement(PlsqlParser.Loop_statementContext ctx) {
        WhileStatement loopStatement = new WhileStatement(TreeNode.Type.WHILE);
        LogicNode conditionNode = null;
        // while statement
        if (ctx.WHILE() != null) {
            visit(ctx.condition());
            conditionNode = (LogicNode) treeBuilder.popStatement();
        } else if (ctx.FOR() != null) {
            // for statement
            visit(ctx.cursor_loop_param());
            conditionNode = (LogicNode) treeBuilder.popStatement();
        } else {
            // basic loop condition always be true
            conditionNode = new LogicNode();
            conditionNode.setBool(true);
        }
        loopStatement.setCondtionNode(conditionNode);
        visit(ctx.seq_of_statements());
        treeBuilder.addNode(loopStatement);
        treeBuilder.pushStatement(loopStatement);
        return loopStatement;
    }

    @Override
    public Object visitCursor_loop_param(PlsqlParser.Cursor_loop_paramContext ctx) {
        LogicNode andNode = new LogicNode(TreeNode.Type.AND);
        LogicNode leftNotNode = new LogicNode(TreeNode.Type.NOT);
        LogicNode rightNotNode = new LogicNode(TreeNode.Type.NOT);
        andNode.addNode(leftNotNode);
        andNode.addNode(rightNotNode);
        PredicateNode leftPredicateNode = new PredicateNode(TreeNode.Type.PREDICATE);
        leftNotNode.addNode(leftPredicateNode);
        PredicateNode rightPredicateNode = new PredicateNode(TreeNode.Type.PREDICATE);
        rightNotNode.addNode(rightNotNode);
        if (ctx.index_name() != null) {
            // number seq
            leftPredicateNode.setOp(">=");
            rightPredicateNode.setOp("<=");
            if (ctx.REVERSE() != null) {
            }
            visit(ctx.index_name());
            TreeNode indexExprNode = treeBuilder.popStatement();
            leftPredicateNode.addNode(indexExprNode);
            visit(ctx.lower_bound());
            leftPredicateNode.addNode(treeBuilder.popStatement());
            visit(ctx.upper_bound());
            rightPredicateNode.addNode(indexExprNode);
            rightPredicateNode.addNode(treeBuilder.popStatement());
        } else if (ctx.record_name() != null) {
            // cursor seq
        } else {
            // non exists
        }
        return andNode;
    }

    private ExpressionStatement genExpression(String name, Object value, Var.DataType dataType) {
        ExpressionBean expressionBean = new ExpressionBean();
        Var var = new Var(value, dataType);
        expressionBean.setVar(var);
        return new ExpressionStatement(expressionBean);
    }

    @Override
    public Object visitRegular_id(PlsqlParser.Regular_idContext ctx) {
//        Var id = new Var(ctx.getText(), Var.DataType.INT);
        ExpressionStatement expressionStatement = genExpression(ctx.getText(), null, Var.DataType.DEFAULT);
        treeBuilder.pushStatement(expressionStatement);
        return expressionStatement;
    }

    @Override
    public Object visitConcatenation(PlsqlParser.ConcatenationContext ctx) {
        // TODO test only constant
        System.out.println("get here");
        return visitChildren(ctx);
    }

    // TODO only for test, all function call just only print args
    @Override
    public Object visitFunction_call(PlsqlParser.Function_callContext ctx) {
        Function function = new Function();
        visit(ctx.routine_name());
        treeBuilder.popAll();
        if (ctx.function_argument() != null) {
            List<Var> args = (List<Var>) visit(ctx.function_argument());
            function.setVars(args);
        }
        treeBuilder.pushStatement(function);
        return function;
    }

    @Override
    public Object visitFunction_argument(PlsqlParser.Function_argumentContext ctx) {
        List<Var> args = new ArrayList<Var>();
        for (PlsqlParser.ArgumentContext argCtx: ctx.argument()) {
            Var var = (Var) visit(argCtx);
            treeBuilder.popStatement();
            args.add(var);
        }
        return args;
    }

    /*@Override
    public Object visitArgument(PlsqlParser.ArgumentContext ctx) {
        return visitChildren(ctx);
    }*/

    @Override
    public Object visitConstant(PlsqlParser.ConstantContext ctx) {
        ExpressionBean expressionBean = new ExpressionBean();
        Var var = null;
        if (ctx.numeric() != null) {
            var = (Var) visit(ctx.numeric());
        } else {
            var = new Var("", ctx.getText(), Var.DataType.STRING);
        }
        expressionBean.setVar(var);
        ExpressionStatement expressionStatement = new ExpressionStatement(expressionBean);
        treeBuilder.pushStatement(expressionStatement);
        return var;
    }

    @Override
    public Object visitNumeric(PlsqlParser.NumericContext ctx) {
        Var val = new Var();
        if (ctx.UNSIGNED_INTEGER() != null) {
            // integer
            val.setVarValue(ctx.getText());
            val.setDataType(Var.DataType.INT);
        } else {
            // float
            val.setVarValue(ctx.getText());
            val.setDataType(Var.DataType.FLOAT);
        }
        return val;
    }
}
