package org.apache.hive.plsql;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.StringUtils;
import org.apache.hive.basesql.TreeBuilder;
import org.apache.hive.plsql.block.AnonymousBlock;
import org.apache.hive.plsql.block.ExceptionHandler;
import org.apache.hive.plsql.cfl.OracleRaiseStatement;
import org.apache.hive.plsql.cfl.OracleReturnStatement;
import org.apache.hive.plsql.cursor.*;
import org.apache.hive.plsql.ddl.commonFragment.CrudTableFragment;
import org.apache.hive.plsql.ddl.fragment.alterTableFragment.*;
import org.apache.hive.plsql.ddl.fragment.createTableFragment.OracleCreateTableStatement;
import org.apache.hive.plsql.ddl.fragment.createViewFragment.OracleCreateViewStatment;
import org.apache.hive.plsql.ddl.fragment.dropTruckTableFm.OracleDropTableStatement;
import org.apache.hive.plsql.ddl.fragment.dropTruckTableFm.OracleDropViewStatement;
import org.apache.hive.plsql.ddl.fragment.dropTruckTableFm.OracleTruncateTableStatement;
import org.apache.hive.plsql.ddl.fragment.dropTruckTableFm.OracleUseStatement;
import org.apache.hive.plsql.ddl.fragment.packageFragment.*;
import org.apache.hive.plsql.dml.OracleSelectStatement;
import org.apache.hive.plsql.dml.commonFragment.*;
import org.apache.hive.plsql.dml.fragment.delFragment.OracleDelStatement;
import org.apache.hive.plsql.dml.fragment.explainFragment.OracleExplainStatment;
import org.apache.hive.plsql.dml.fragment.insertFragment.*;
import org.apache.hive.plsql.dml.fragment.mergeFragment.*;
import org.apache.hive.plsql.dml.fragment.selectFragment.*;
import org.apache.hive.plsql.dml.fragment.selectFragment.groupByFragment.GroupByElemFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.groupByFragment.GroupByFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.groupByFragment.HavingClauseFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.joinFragment.JoinClauseFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.joinFragment.JoinOnPartFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.joinFragment.JoinUsingPartFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.pivotFragment.*;
import org.apache.hive.plsql.dml.fragment.selectFragment.tableRefFragment.GeneralTableRefFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.tableRefFragment.TableRefAuxFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.tableRefFragment.TableRefFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.tableRefFragment.TableRefListFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.unpivotFragment.UnpivotClauseFragment;
import org.apache.hive.plsql.dml.fragment.selectFragment.unpivotFragment.UnpivotInClauseFm;
import org.apache.hive.plsql.dml.fragment.selectFragment.unpivotFragment.UnpivotInElementsFm;
import org.apache.hive.plsql.dml.fragment.updateFragment.ColumnBasedUpDateFm;
import org.apache.hive.plsql.dml.fragment.updateFragment.OracleUpdateStatement;
import org.apache.hive.plsql.dml.fragment.updateFragment.StaticReturningClauseFm;
import org.apache.hive.plsql.dml.fragment.updateFragment.UpdateSetClauseFm;
import org.apache.hive.plsql.expression.GeneralExpression;
import org.apache.hive.plsql.function.FakeFunction;
import org.apache.hive.plsql.function.Function;
import org.apache.hive.plsql.function.ProcedureCall;
import org.apache.hive.plsql.type.LocalTypeDeclare;
import org.apache.hive.plsql.type.RecordTypeDeclare;
import org.apache.hive.plsql.type.TableTypeDeclare;
import org.apache.hive.tsql.TSqlParser;
import org.apache.hive.tsql.another.DeclareStatement;
import org.apache.hive.tsql.another.SetStatement;
import org.apache.hive.tsql.another.UseStatement;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.cfl.*;
import org.apache.hive.tsql.common.*;
import org.apache.hive.tsql.cursor.DeclareCursorStatement;
import org.apache.hive.tsql.ddl.CreateFunctionStatement;
import org.apache.hive.tsql.ddl.CreateProcedureStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;
import org.apache.hive.tsql.exception.Position;
import org.apache.hive.tsql.func.FuncName;
import org.apache.hive.tsql.func.Procedure;
import org.apache.hive.tsql.node.LogicNode;
import org.apache.hive.tsql.node.PredicateNode;
import org.apache.spark.sql.catalyst.expressions.Expression;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Created by dengrb1 on 3/31 0031.
 */
public class PLsqlVisitorImpl extends PlsqlBaseVisitor<Object> {
    private TreeBuilder treeBuilder = null;
    // check duplicate names
    private HashSet<String> variableNames = new HashSet<>();

    public PLsqlVisitorImpl(TreeNode rootNode) {
        treeBuilder = new TreeBuilder(rootNode);
    }

    public List<Exception> getExceptions() {
        return treeBuilder.getExceptions();
    }

    @Override
    public Object visitCompilation_unit(PlsqlParser.Compilation_unitContext ctx) {
        for (PlsqlParser.Unit_statementContext unitCtx : ctx.unit_statement()) {
            visit(unitCtx);
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
        List<ParseTree> childs = ctx.children;
        for (ParseTree child : childs) {
            if (child instanceof PlsqlParser.Label_declarationContext) {
                visit((PlsqlParser.Label_declarationContext) child);
                treeBuilder.addNode(beginEndStatement);
            }
            if (child instanceof PlsqlParser.StatementContext) {
                visit((PlsqlParser.StatementContext) child);
                treeBuilder.addNode(beginEndStatement);
            }
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
    public Object visitAssignment_statement(PlsqlParser.Assignment_statementContext ctx) {
        SetStatement setStatement = new SetStatement();
        String varName = "";
        if (ctx.general_element() != null) {
            varName = ctx.general_element().getText();
        }
        visit(ctx.expression());
        TreeNode expr = treeBuilder.popStatement();
        Var var = new Var(varName, expr);
        var.setValueType(Var.ValueType.EXPRESSION);
        setStatement.setVar(var);
        treeBuilder.pushStatement(setStatement);
        return setStatement;
    }

    @Override
    public Object visitAnonymous_block(PlsqlParser.Anonymous_blockContext ctx) {
        AnonymousBlock anonymousBlock = new AnonymousBlock(TreeNode.Type.ANONY_BLOCK);
        for (PlsqlParser.Declare_specContext declareCtx : ctx.declare_spec()) {
            visit(declareCtx);
            treeBuilder.addNode(anonymousBlock);
        }
        variableNames.clear();

        /*if (ctx.body() != null) {
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
        }*/
        visit(ctx.body().seq_of_statements());
        TreeNode block = treeBuilder.popStatement();
        anonymousBlock.addNode(block);
        for (PlsqlParser.Exception_handlerContext exception_handlerContext : ctx.body().exception_handler()) {
            visit(exception_handlerContext);
            ExceptionHandler handler = (ExceptionHandler) treeBuilder.popStatement();
            anonymousBlock.addExecptionNode(handler);
        }

        treeBuilder.pushStatement(anonymousBlock);
        return anonymousBlock;
    }

    @Override
    public Object visitRaise_statement(PlsqlParser.Raise_statementContext ctx) {
        OracleRaiseStatement raiseStatement = new OracleRaiseStatement(TreeNode.Type.ORACLE_RAISE);
        raiseStatement.setExceptionName(ctx.exception_name().getText());
        treeBuilder.pushStatement(raiseStatement);
        return raiseStatement;
    }

    @Override
    public Object visitException_handler(PlsqlParser.Exception_handlerContext ctx) {
        ExceptionHandler exceptionHandler = new ExceptionHandler();
        for (PlsqlParser.Exception_nameContext ctxNames : ctx.exception_name()) {
            String exceptionName = ctxNames.getText();
            exceptionHandler.setExceptionName(exceptionName);
        }
        visit(ctx.seq_of_statements());
        TreeNode exceptionBlock = treeBuilder.popStatement();
        exceptionHandler.setStmts(exceptionBlock);
        treeBuilder.pushStatement(exceptionHandler);
        return exceptionHandler;
    }

    /*@Override
    public Object visitDeclare_spec(PlsqlParser.Declare_specContext ctx) {
        return visitChildren(ctx);
    }*/

    private void checkDuplicateVariable(String name, ParserRuleContext ctx) {
        if (variableNames.contains(name))
            treeBuilder.addException(name + " is duplicate", locate(ctx));
        else
            variableNames.add(name);
    }

    @Override
    public Object visitException_declaration(PlsqlParser.Exception_declarationContext ctx) {
        DeclareStatement declareStatement = new DeclareStatement();
        Var var = new Var();
        String exceptionName = ctx.exception_name().getText();
        checkDuplicateVariable(exceptionName, ctx);
        var.setVarName(exceptionName);
        var.setDataType(Var.DataType.EXCEPTION);
        declareStatement.addDeclareVar(var);
        treeBuilder.pushStatement(declareStatement);
        return declareStatement;
    }

    @Override
    public Object visitVariable_declaration(PlsqlParser.Variable_declarationContext ctx) {
        DeclareStatement declareStatement = new DeclareStatement();
        Var var = null;
        String varName = ctx.variable_name().getText();
        checkDuplicateVariable(varName, ctx);
        var = genVarBasedTypeSpec(varName, ctx.type_spec());
//        var.setVarName(varName);
//        Var.DataType varType = (Var.DataType) visit(ctx.type_spec());
//        var.setDataType(varType);
        if (ctx.default_value_part() != null) {
            visit(ctx.default_value_part());
            var.setExpr(treeBuilder.popStatement());
            var.setValueType(Var.ValueType.EXPRESSION);
        }
        declareStatement.addDeclareVar(var);
        treeBuilder.pushStatement(declareStatement);
        return declareStatement;
    }

    private Var genVarBasedTypeSpec(String varName, PlsqlParser.Type_specContext type_specCtx) {
        Var var = new Var();
        var.setVarName(varName);
        Var.DataType dataType = (Var.DataType) visit(type_specCtx);
        var.setDataType(dataType);
        if (dataType == Var.DataType.REF || dataType == Var.DataType.COMPLEX || dataType == Var.DataType.CUSTOM) {
            var.setRefTypeName(type_specCtx.type_name().getText());
        }
        return var;
    }

    @Override
    public Var.DataType visitType_spec(PlsqlParser.Type_specContext ctx) {
        String dataType = "";
        // primitive type
        if (ctx.datatype() != null) {
            // receive from native_datatype_element only, abandon precision_part
//            typeName = (String) visit(ctx.datatype());
            dataType = (String) visitNative_datatype_element(ctx.datatype().native_datatype_element());
            dataType = dataType.toUpperCase();
            if (dataType.contains("BIGINT")) {
                return Var.DataType.LONG;
            } else if (dataType.contains("INT") || dataType.contains("NUMBER")) {
                return Var.DataType.INT;
            } else if (dataType.contains("BINARY")) {
                return Var.DataType.BINARY;
            } else if (dataType.contains("DATETIME") || dataType.contains("TIMESTAMP")) {
                return Var.DataType.DATETIME;
            } else if (dataType.equalsIgnoreCase("TIME")) {
                return Var.DataType.TIME;
            } else if (dataType.contains("DATE")) {
                return Var.DataType.DATE;
            } else if (dataType.contains("CHAR") || dataType.contains("TEXT") || dataType.contains("NCHAR")) {
                return Var.DataType.STRING;
            } else if (dataType.contains("FLOAT") || dataType.contains("REAL")) {
                return Var.DataType.FLOAT;
            } else if (dataType.contains("BIT")
                    || dataType.contains("XML")
                    || dataType.contains("IMAGE")
                    || dataType.contains("UNIQUEIDENTIFIER")
                    || dataType.contains("GEOGRAPHY")
                    || dataType.contains("GEOMETRY")
                    || dataType.contains("HIERARCHYID")) {
                treeBuilder.addException(dataType, locate(ctx));
                return Var.DataType.NULL;
            } else if (dataType.contains("MONEY") || dataType.contains("DECIMAL")
                    || dataType.contains("NUMERIC")) {
                return Var.DataType.DOUBLE;
            } else {
                return Var.DataType.STRING;
            }
        }

        // complex Type
        if (ctx.type_name() != null) {
            if (ctx.PERCENT_ROWTYPE() != null)
                return Var.DataType.COMPLEX;
            if (ctx.PERCENT_TYPE() != null)
                return Var.DataType.REF;
            // local type declare
            return Var.DataType.CUSTOM;
        }
        return Var.DataType.DEFAULT;
    }

    @Override
    public Object visitNative_datatype_element(PlsqlParser.Native_datatype_elementContext ctx) {
        return ctx.getText();
    }

    @Override
    public Object visitGeneral_element_part(PlsqlParser.General_element_partContext ctx) {
        GeneralExpression generalExpression = new GeneralExpression();
        for (PlsqlParser.Id_expressionContext id_expressionCtx : ctx.id_expression()) {
            visit(id_expressionCtx);
            ExpressionStatement es = (ExpressionStatement) treeBuilder.popStatement();
            generalExpression.addGeneralExpr(es);
        }
        if (ctx.function_argument() != null) {
            List<Var> list = (List<Var>) visitFunction_argument(ctx.function_argument());
            if (list.size() != 1)
                treeBuilder.addException("general element index error", locate(ctx));
            ExpressionStatement expressionStatement = new ExpressionStatement();
            ExpressionBean eb = new ExpressionBean();
            Var indexVar = list.get(0);
            eb.setVar(indexVar);
            expressionStatement.setExpressionBean(eb);
            generalExpression.addGeneralExpr(expressionStatement);
        }
        treeBuilder.pushStatement(generalExpression);
        return generalExpression;
    }

    @Override
    public Object visitGeneral_element(PlsqlParser.General_elementContext ctx) {
        GeneralExpression generalExpression = new GeneralExpression();
        for (PlsqlParser.General_element_partContext partCtx : ctx.general_element_part()) {
            visit(partCtx);
            GeneralExpression partExpr = (GeneralExpression) treeBuilder.popStatement();
            generalExpression.mergeGeneralExpr(partExpr);
        }
        treeBuilder.pushStatement(generalExpression);
        return generalExpression;
    }

    /*@Override
    public Object visitAtom(PlsqlParser.AtomContext ctx) {
        if (ctx.general_element() != null)
            visit(ctx.general_element());
        return visitChildren(ctx);
    }*/

    @Override
    public Object visitStandard_function(PlsqlParser.Standard_functionContext ctx) {
        // TODO only implement cursor attribute
        OracleCursorAttribute ca = new OracleCursorAttribute();
        if (ctx.cursor_name() != null) {
            ca.setCursorName(ctx.cursor_name().getText());
            if (ctx.PERCENT_FOUND() != null)
                ca.setMark(ctx.PERCENT_FOUND().getText());
            else if (ctx.PERCENT_NOTFOUND() != null)
                ca.setMark(ctx.PERCENT_NOTFOUND().getText());
            else if (ctx.PERCENT_ISOPEN() != null)
                ca.setMark(ctx.PERCENT_ISOPEN().getText());
            else if (ctx.PERCENT_ROWCOUNT() != null)
                ca.setMark(ctx.PERCENT_ROWCOUNT().getText());
            treeBuilder.pushStatement(ca);
        }
        return ca;
    }

    @Override
    public Object visitUnary_expression(PlsqlParser.Unary_expressionContext ctx) {
        ExpressionStatement unaryEs = null;
        // TODO only implement standard funciton
        if (ctx.standard_function() != null) {
            visit(ctx.standard_function());
            unaryEs = (ExpressionStatement) treeBuilder.popStatement();
        }
        // TODO only implement general_element_part for x(i).y
        if (ctx.atom() != null) {
            visit(ctx.atom());
            unaryEs = (ExpressionStatement) treeBuilder.popStatement();
        }
        treeBuilder.pushStatement(unaryEs);
        return unaryEs;
    }

    /*@Override
    public Object visitUnary_expression_alias(PlsqlParser.Unary_expression_aliasContext ctx) {
        // need generate a ExpressionStatement
        visit(ctx.unary_expression());
        ExpressionStatement unaryEs = (ExpressionStatement) treeBuilder.popStatement();
        treeBuilder.pushStatement(unaryEs);
        return unaryEs;
    }

    @Override
    public Object visitUnary_expression_subalias(PlsqlParser.Unary_expression_subaliasContext ctx) {
        return visitChildren(ctx);
    }*/

    @Override
    public Object visitBool_condition_alias(PlsqlParser.Bool_condition_aliasContext ctx) {
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
    public Object visitBinary_expression_alias(PlsqlParser.Binary_expression_aliasContext ctx) {
        ExpressionBean expressionBean = new ExpressionBean();
        ExpressionStatement es = new ExpressionStatement(expressionBean);
        expressionBean.setOperatorSign(OperatorSign.getOpator(ctx.op.getText()));
        List<PlsqlParser.ExpressionContext> exprList = ctx.expression();
        if (exprList.size() != 2) {
            // TODO exception
        }
        for (PlsqlParser.ExpressionContext expr : exprList) {
            visit(expr);
            treeBuilder.addNode(es);
        }
        treeBuilder.pushStatement(es);
        return es;
    }

    @Override
    public Object visitBinary_expression_subalias(PlsqlParser.Binary_expression_subaliasContext ctx) {
        ExpressionBean expressionBean = new ExpressionBean();
        ExpressionStatement es = new ExpressionStatement(expressionBean);
        expressionBean.setOperatorSign(OperatorSign.getOpator(ctx.op.getText()));
        List<PlsqlParser.Sub_expressionContext> exprList = ctx.sub_expression();
        if (exprList.size() != 2) {
            // TODO exception
        }
        for (PlsqlParser.Sub_expressionContext expr : exprList) {
            visit(expr);
            treeBuilder.addNode(es);
        }
        treeBuilder.pushStatement(es);
        return es;
    }

    private ExpressionStatement genId_expression(List<PlsqlParser.Id_expressionContext> exprs) {
        ExpressionBean expressionBean = new ExpressionBean();
        ExpressionStatement expressionStatement = new ExpressionStatement(expressionBean);
        Var var = new Var();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < exprs.size(); i++) {
            sb.append(exprs.get(i).getText());
            if (i < exprs.size() - 1)
                sb.append(".");
        }
        var.setVarName(sb.toString());
        var.setDataType(Var.DataType.VAR);
        expressionBean.setVar(var);
        return expressionStatement;
    }

    @Override
    public Object visitId_expression_alias(PlsqlParser.Id_expression_aliasContext ctx) {
        /*ExpressionBean expressionBean = new ExpressionBean();
        ExpressionStatement expressionStatement = new ExpressionStatement(expressionBean);
        Var var = new Var();
        String varName = ctx.getText();
        var.setVarName(varName);
        var.setDataType(Var.DataType.VAR);
        expressionBean.setVar(var);*/
        ExpressionStatement expressionStatement = genId_expression(ctx.id_expression());
        treeBuilder.pushStatement(expressionStatement);
        return expressionStatement;
    }

    @Override
    public Object visitId_expression_subalias(PlsqlParser.Id_expression_subaliasContext ctx) {
        ExpressionStatement expressionStatement = genId_expression(ctx.id_expression());
        treeBuilder.pushStatement(expressionStatement);
        return expressionStatement;
    }

    private ExpressionStatement genConstant(PlsqlParser.ConstantContext ctx) {
        visit(ctx);
        return (ExpressionStatement) treeBuilder.popStatement();
    }

    @Override
    public Object visitConstant_alias(PlsqlParser.Constant_aliasContext ctx) {
        if (ctx.constant() != null) {
        }
        TreeNode es = genConstant(ctx.constant());
        treeBuilder.pushStatement(es);
        return es;
    }

    @Override
    public Object visitConstant_subalias(PlsqlParser.Constant_subaliasContext ctx) {
        if (ctx.constant() != null) {
            // TODO
        }
        TreeNode es = genConstant(ctx.constant());
        treeBuilder.pushStatement(es);
        return es;
    }

    /*@Override
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
    }*/
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
        if (ctx.NOT() != null) {
            orNode.setNot();
        }
        visit(ctx.equality_expression());
        treeBuilder.addNode(orNode);
        treeBuilder.pushStatement(orNode);
        return orNode;
    }

    @Override
    public Object visitRelational_expression(PlsqlParser.Relational_expressionContext ctx) {
        List<PlsqlParser.Compound_expressionContext> expressCtxList = ctx.compound_expression();
        if (expressCtxList.size() == 1) {
            return visit(expressCtxList.get(0));
        }
        if (expressCtxList.size() != 2) {
            return null;
        }
        PredicateNode predicateNode = new PredicateNode(TreeNode.Type.PREDICATE);
        predicateNode.setEvalType(PredicateNode.CompType.COMP);
        String op = ctx.relational_operator().getText();
        predicateNode.setOp(op);
        for (PlsqlParser.Compound_expressionContext expressionContext : expressCtxList) {
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
            TreeNode conditionNode = treeBuilder.popStatement();
            ifStatement.setCondtion(conditionNode);
        }
        if (ctx.seq_of_statements() != null) {
            visit(ctx.seq_of_statements());
            treeBuilder.addNode(ifStatement);
        }
        for (PlsqlParser.Elsif_partContext elsifCtx : ctx.elsif_part()) {
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
    public Object visitContinue_statement(PlsqlParser.Continue_statementContext ctx) {
        ContinueStatement continueStmt = new ContinueStatement(TreeNode.Type.CONTINUE);
        if (ctx.label_name() != null) {
            continueStmt.setLabel(ctx.label_name().getText());
        }
        if (ctx.condition() != null) {
            visit(ctx.condition());
            LogicNode conditionNode = (LogicNode) treeBuilder.popStatement();
            continueStmt.setCondition(conditionNode);
        }
        treeBuilder.pushStatement(continueStmt);
        return continueStmt;
    }

    @Override
    public Object visitExit_statement(PlsqlParser.Exit_statementContext ctx) {
        BreakStatement exitStmt = new BreakStatement(TreeNode.Type.BREAK);
        if (ctx.label_name() != null) {
            exitStmt.setLabel(ctx.label_name().getText());
        }
        if (ctx.condition() != null) {
            visit(ctx.condition());
            LogicNode conditionNode = (LogicNode) treeBuilder.popStatement();
            exitStmt.setCondition(conditionNode);
        }
        treeBuilder.pushStatement(exitStmt);
        return exitStmt;
    }

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
//        loopStatement.setLoopIndexVar(conditionNode.getIndexVar());
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
        leftPredicateNode.setEvalType(PredicateNode.CompType.COMP);
        leftNotNode.addNode(leftPredicateNode);
        PredicateNode rightPredicateNode = new PredicateNode(TreeNode.Type.PREDICATE);
        rightPredicateNode.setEvalType(PredicateNode.CompType.COMP);
        rightNotNode.addNode(rightPredicateNode);
        if (ctx.index_name() != null) {
            // number seq
            leftPredicateNode.setOp(">=");
            rightPredicateNode.setOp("<=");
            //ExpressionStatement indexExprNode = visitIndex_name(ctx.index_name());
            visit(ctx.index_name());
            ExpressionStatement indexExprNode = (ExpressionStatement) treeBuilder.popStatement();
            indexExprNode.getExpressionBean().getVar().setDataType(Var.DataType.INT);
            indexExprNode.getExpressionBean().getVar().setReadonly(true);
            leftPredicateNode.addNode(indexExprNode);
            visit(ctx.lower_bound());
            ExpressionStatement lowerStmt = (ExpressionStatement) treeBuilder.popStatement();
            leftPredicateNode.addNode(lowerStmt);
            visit(ctx.upper_bound());
            ExpressionStatement upperStmt = (ExpressionStatement) treeBuilder.popStatement();
            rightPredicateNode.addNode(indexExprNode);
            rightPredicateNode.addNode(upperStmt);
            genLoopIndex(andNode, indexExprNode, lowerStmt, upperStmt, ctx.REVERSE() != null);
        } else if (ctx.record_name() != null) {
            // cursor seq
        } else {
            // non exists
        }
        treeBuilder.pushStatement(andNode);
        return andNode;
    }

    private void genLoopIndex(LogicNode condition, ExpressionStatement indexStmt,
                              ExpressionStatement lowerStmt, ExpressionStatement upperStmt, boolean dir) {
        try {
            LogicNode.IndexIterator indexIterator = new LogicNode.IndexIterator();
            indexIterator.setIndexVar(indexStmt.getExpressionBean().getVar());
            indexIterator.setLower(lowerStmt.getExpressionBean().getVar());
            indexIterator.setUpper(upperStmt.getExpressionBean().getVar());
            indexIterator.init();
            condition.setIndexIter(indexIterator);
            if (dir)
                indexIterator.setReverse();
        } catch (Exception e) {
            // TODO add exception
            e.printStackTrace();
        }
    }

    @Override
    public Object visitIndex_name(PlsqlParser.Index_nameContext ctx) {
        visit(ctx.id_expression());
        ExpressionStatement indexName = (ExpressionStatement) treeBuilder.popStatement();
        treeBuilder.pushStatement(indexName);
        return indexName;
    }

    private ExpressionStatement genExpression(String name, Object value, Var.DataType dataType) {
        ExpressionBean expressionBean = new ExpressionBean();
        Var var = new Var(name, value, dataType);
        expressionBean.setVar(var);
        return new ExpressionStatement(expressionBean);
    }

    @Override
    public Object visitRegular_id(PlsqlParser.Regular_idContext ctx) {
        ExpressionStatement expressionStatement = genExpression(ctx.getText(), null, Var.DataType.VAR);
        treeBuilder.pushStatement(expressionStatement);
        return expressionStatement;
    }

    @Override
    public Object visitCreate_procedure_body(PlsqlParser.Create_procedure_bodyContext ctx) {
        String sourceProcedure = ctx.start.getInputStream().getText(
                new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
        String procName = ctx.procedure_name().getText();
        FuncName procId = new FuncName();
        procId.setFuncName(procName);
        Procedure procedure = new Procedure(procId);
        procedure.setProcSql(sourceProcedure);
        procedure.setMd5(MD5Util.md5Hex(sourceProcedure));
        for (PlsqlParser.ParameterContext para : ctx.parameter()) {
            procedure.addInAndOutputs((Var) visit(para));
        }
        visit(ctx.anonymous_block());
        procedure.setSqlClauses(treeBuilder.popStatement());
        procedure.setLeastArguments();
        CreateProcedureStatement.Action action = null != ctx.CREATE() ?
                CreateProcedureStatement.Action.CREATE : CreateProcedureStatement.Action.ALTER;
        CreateProcedureStatement statement = new CreateProcedureStatement(procedure, action);
        treeBuilder.pushStatement(statement);
        return statement;
    }

    @Override
    public Object visitParameter(PlsqlParser.ParameterContext ctx) {
        String varName = ctx.parameter_name().getText().toUpperCase();
        Var.DataType type = Var.DataType.DEFAULT;
        if (ctx.type_spec() != null) {
            type = (Var.DataType) visit(ctx.type_spec());
        }
        Var var = new Var(varName, null, type);
        if (ctx.OUT().size() != 0)
            var.setVarType(Var.VarType.OUTPUT);
        if (ctx.IN().size() != 0 && ctx.OUT().size() != 0)
            var.setVarType(Var.VarType.INOUT);
        if (ctx.INOUT().size() != 0)
            var.setVarType(Var.VarType.INOUT);
        if (ctx.NOCOPY().size() != 0)
            var.setNoCopy();
        if (ctx.default_value_part() != null) {
            visit(ctx.default_value_part().expression());
            TreeNode expressionStatement = treeBuilder.popStatement();
            var.setExpr(expressionStatement);
            var.setValueType(Var.ValueType.EXPRESSION);
            var.setDefault(true);
        }
        return var;
    }

    @Override
    public Object visitCreate_function_body(PlsqlParser.Create_function_bodyContext ctx) {
        String sourceFunction = ctx.start.getInputStream().getText(
                new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
        List<PlsqlParser.Id_expressionContext> idExprs = ctx.function_name().id_expression();
        String functionName = null;
        String dbName = null;
        if(idExprs.size() == 1){
            functionName = idExprs.get(0).getText();
        } else if(idExprs.size() ==2){
            dbName = idExprs.get(0).getText();
            functionName = idExprs.get(1).getText();
        }
        FuncName funcId = new FuncName();
        funcId.setFuncName(functionName);
        if(dbName != null){
            funcId.setDatabase(dbName);
        }
        Function function = new Function(funcId);
        function.setProcSql(sourceFunction);
        function.setMd5(MD5Util.md5Hex(sourceFunction));
        for (PlsqlParser.ParameterContext para : ctx.parameter()) {
            function.addInAndOutputs((Var) visit(para));
        }
        visit(ctx.anonymous_block());
        function.setSqlClauses(treeBuilder.popStatement());
        function.setLeastArguments();
        CreateFunctionStatement.Action create = null != ctx.CREATE() ? CreateFunctionStatement.Action.CREATE : null;
        CreateFunctionStatement.Action replace = null != ctx.REPLACE() ? CreateFunctionStatement.Action.REPLACE : null;
        String returnType = ctx.type_spec() != null ? ctx.type_spec().getText().toUpperCase() : "NONE";
        CreateFunctionStatement.SupportDataTypes rt = CreateFunctionStatement.fromString(returnType);
        if (rt == null) {
            treeBuilder.addException("Not Supported Type " + returnType, ctx);
            return null;
        }
        CreateFunctionStatement statement = new CreateFunctionStatement(function, create, replace, rt);
        treeBuilder.pushStatement(statement);
        return statement;
    }

    @Override
    public Object visitReturn_statement(PlsqlParser.Return_statementContext ctx) {
        OracleReturnStatement returnStatement = new OracleReturnStatement();
        if (ctx.condition() != null) {
            visit(ctx.condition());
            returnStatement.setExpr(treeBuilder.popStatement());
        }
        treeBuilder.pushStatement(returnStatement);
        return returnStatement;
    }

    /*@Override
    public Object visitConcatenation(PlsqlParser.ConcatenationContext ctx) {
        // TODO test only constant
        System.out.println("get here");
        return visitChildren(ctx);
    }*/
    // TODO only for test, print | DBMS_OUTPUT.put_line function call just only print args
    @Override
    public Object visitFunction_call(PlsqlParser.Function_callContext ctx) {
        String name = ctx.routine_name().getText().toLowerCase();
        if (name.startsWith("print") || name.startsWith("dbms_output.put_line")) {
            FakeFunction function = new FakeFunction();
            visit(ctx.routine_name());
            treeBuilder.popAll();
            List<Var> vars = new ArrayList<>();
            for (PlsqlParser.Execute_argumentContext argCtx : ctx.execute_argument()) {
                vars.add((Var) visit(argCtx));
            }
            function.setVars(vars);
            /*if (ctx.execute_argument() != null) {
                List<Var> args = (List<Var>) visit(ctx.function_argument());
                function.setVars(args);
            }*/
            treeBuilder.pushStatement(function);
            return function;
        } else {
            FuncName funcName = new FuncName();
            funcName.setFuncName(name);
            ProcedureCall statement = new ProcedureCall(funcName);
            for (PlsqlParser.Execute_argumentContext argCtx : ctx.execute_argument()) {
                Var arg = (Var) visit(argCtx);
                statement.addArgument(arg);
            }
            treeBuilder.pushStatement(statement);
            return statement;
        }
        /*FakeFunction function = new FakeFunction();
        visit(ctx.routine_name());
        treeBuilder.popAll();
        if (ctx.function_argument() != null) {
            List<Var> args = (List<Var>) visit(ctx.function_argument());
            function.setVars(args);
        }
        treeBuilder.pushStatement(function);
        return function;*/
    }

    @Override
    public Object visitExecute_argument(PlsqlParser.Execute_argumentContext ctx) {
        Var var = new Var();
        if (ctx.id_expression() != null) {
//            var.setMapName(ctx.id_expression().getText());
            var.setVarName(ctx.id_expression().getText());
        }
        visit(ctx.argument());
        TreeNode expressionStatement = treeBuilder.popStatement();
        var.setExpr(expressionStatement);
        var.setValueType(Var.ValueType.EXPRESSION);
        return var;
    }

    @Override
    public Object visitFunction_argument(PlsqlParser.Function_argumentContext ctx) {
        List<Var> args = new ArrayList<Var>();
        for (PlsqlParser.ArgumentContext argCtx : ctx.argument()) {
            visit(argCtx);
            ExpressionStatement expressionStatement = (ExpressionStatement) treeBuilder.popStatement();
            args.add(expressionStatement.getExpressionBean().getVar());
        }
        return args;
    }

    /*@Override
    public Object visitArgument(PlsqlParser.ArgumentContext ctx) {
        return visitChildren(ctx);
    }*/
    @Override
    public ExpressionStatement visitConstant(PlsqlParser.ConstantContext ctx) {
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
        return expressionStatement;
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

    private String genTableColString(List<PlsqlParser.Column_nameContext> colCtxs,
                                     List<PlsqlParser.Type_specContext> typeCtxs) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < colCtxs.size(); i++) {
            String colName = colCtxs.get(i).getText();
            Var.DataType type = (Var.DataType) visit(typeCtxs.get(i));
            String typeName = type.toString();
            sb.append(colName).append(Common.SPACE).append(typeName);
            if (i < colCtxs.size() - 1)
                sb.append(", ");
        }
        return sb.toString();
    }

    /**
     * create_table
     * : CREATE  (GLOBAL TEMPORARY)? TABLE tableview_name
     * '(' column_name type_spec column_constraint?
     * (',' column_name type_spec column_constraint?)* ')' crud_table?
     * table_space? storage?
     * tmp_tb_comments?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public OracleCreateTableStatement visitCreate_table(PlsqlParser.Create_tableContext ctx) {
        OracleCreateTableStatement oracleCreateTableStatement = new OracleCreateTableStatement();

        if (null != ctx.GLOBAL()) {
            oracleCreateTableStatement.setTempTable(true);
        }
        visit(ctx.tableview_name());
        TableViewNameFragment tableViewNameFragment = (TableViewNameFragment) treeBuilder.popStatement();
        oracleCreateTableStatement.setTableViewNameFragment(tableViewNameFragment);

        for (PlsqlParser.Column_nameContext column_nameContext : ctx.column_name()) {
            visit(column_nameContext);
            ColumnNameFragment columnNameFragment = (ColumnNameFragment) treeBuilder.popStatement();
            oracleCreateTableStatement.addColumnName(columnNameFragment);
        }

        for (PlsqlParser.Type_specContext type : ctx.type_spec()) {
            Var.DataType columnType = visitType_spec(type);
            oracleCreateTableStatement.addColumnType(columnType);
        }

        if (null != ctx.crud_table()) {
            visit(ctx.crud_table());
            CrudTableFragment crudTableFragment = (CrudTableFragment) treeBuilder.popStatement();
            oracleCreateTableStatement.setCrudTableFragment(crudTableFragment);
        }
        treeBuilder.pushStatement(oracleCreateTableStatement);
        return oracleCreateTableStatement;
    }

    /**
     * crud_table
     * : CLUSTERED BY '(' column_name (',' column_name)* ')' INTO  DECIMAL BUCKETS STORED AS ORC TBLPROPERTIES
     * '('TRANSACTIONAL '=' TRUE')'
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public CrudTableFragment visitCrud_table(PlsqlParser.Crud_tableContext ctx) {
        CrudTableFragment crudTableFragment = new CrudTableFragment();
        for (PlsqlParser.Column_nameContext column_nameContext : ctx.column_name()) {
            visit(column_nameContext);
            ColumnNameFragment columnNameFragment = (ColumnNameFragment) treeBuilder.popStatement();
            crudTableFragment.addColumnName(columnNameFragment);
        }
        String bucketNumber = ctx.numeric().getText();
        crudTableFragment.setBucketNumber(bucketNumber);
        treeBuilder.pushStatement(crudTableFragment);
        return crudTableFragment;
    }

    /*========================================selectStament===============================*/

    /**
     * select_statement
     * : subquery_factoring_clause? subquery (for_update_clause | order_by_clause)?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public OracleSelectStatement visitSelect_statement(PlsqlParser.Select_statementContext ctx) {
        OracleSelectStatement selectStatement = new OracleSelectStatement(Common.SELECT);
        if (ctx.subquery_factoring_clause() != null) {
            visit(ctx.subquery_factoring_clause());
            SubqueryFactoringClause withStatement = (SubqueryFactoringClause) treeBuilder.popStatement();
            selectStatement.setWithQueryStatement(withStatement);
        }
        visit(ctx.subquery());
        SubqueryFragment subqueryStmt = (SubqueryFragment) treeBuilder.popStatement();
        selectStatement.setQueryBlockStatement(subqueryStmt);
        if (ctx.for_update_clause() != null) {
            visit(ctx.for_update_clause());
            //SqlStatement forStmt = (SqlStatement) treeBuilder.popStatement();
            //selectStatement.setForClauseStatement(forStmt);
        }
        if (ctx.order_by_clause() != null) {
            visit(ctx.order_by_clause());
            OrderByClauseFragment orderStmt = (OrderByClauseFragment) treeBuilder.popStatement();
            selectStatement.setOrderByStatement(orderStmt);
        }
        treeBuilder.pushStatement(selectStatement);
        return selectStatement;
    }

    /**
     * subquery_factoring_clause
     * : WITH factoring_element (',' factoring_element)*
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public SubqueryFactoringClause visitSubquery_factoring_clause(PlsqlParser.Subquery_factoring_clauseContext ctx) {
        SubqueryFactoringClause subqueryFactoringClause = new SubqueryFactoringClause();
        List<FactoringElementFragment> factoringElements = new ArrayList<>();
        for (PlsqlParser.Factoring_elementContext s : ctx.factoring_element()) {
            visit(s);
            FactoringElementFragment factoringElement = (FactoringElementFragment) treeBuilder.popStatement();
            factoringElements.add(factoringElement);
        }
        subqueryFactoringClause.setFactoringElements(factoringElements);
        treeBuilder.pushStatement(subqueryFactoringClause);
        return subqueryFactoringClause;
    }

    /**
     * 广度优先和深度优先
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitSearch_clause(PlsqlParser.Search_clauseContext ctx) {
        treeBuilder.addException(" DEPTH OR  BREADTH  SEARCH ", ctx);
        return null;
    }

    @Override
    public Object visitCycle_clause(PlsqlParser.Cycle_clauseContext ctx) {
        treeBuilder.addException(" Recursive queries  ", ctx);
        return null;
    }

    @Override
    public Object visitFor_update_clause(PlsqlParser.For_update_clauseContext ctx) {
        treeBuilder.addException(" Lock table   ", ctx);
        return null;
    }

    /**
     * column_name
     * : id ('.' id_expression)*
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitColumn_name(PlsqlParser.Column_nameContext ctx) {
        ColumnNameFragment columnNameFragment = new ColumnNameFragment();
        visit(ctx.id());
        IdFragment id = (IdFragment) treeBuilder.popStatement();
        List<ExpressionStatement> idExpressions = new ArrayList<>();
        columnNameFragment.setId(id);
        for (PlsqlParser.Id_expressionContext idExpressionContext : ctx.id_expression()) {
            visit(idExpressionContext);
            ExpressionStatement idExpression = (ExpressionStatement) treeBuilder.popStatement();
            columnNameFragment.addExpress(idExpression);
        }
        treeBuilder.pushStatement(columnNameFragment);
        return columnNameFragment;
    }

    /**
     * factoring_element
     * : query_name ('(' column_name (',' column_name)* ')')? AS '(' subquery order_by_clause? ')'
     * search_clause? cycle_clause?
     *
     * @param ctx
     * @return
     */
    @Override
    public FactoringElementFragment visitFactoring_element(PlsqlParser.Factoring_elementContext ctx) {
        FactoringElementFragment factoringElementFragment = new FactoringElementFragment();
        visit(ctx.query_name());
        IdFragment queryName = (IdFragment) treeBuilder.popStatement();
        factoringElementFragment.setQuereyName(queryName);
        List<ColumnNameFragment> columnNames = new ArrayList<>();
        for (PlsqlParser.Column_nameContext columnNameContext : ctx.column_name()) {
            visit(columnNameContext);
            ColumnNameFragment column = (ColumnNameFragment) treeBuilder.popStatement();
            columnNames.add(column);
        }
        factoringElementFragment.setColumnNames(columnNames);
        visit(ctx.subquery());
        SubqueryFragment subQuery = (SubqueryFragment) treeBuilder.popStatement();
        factoringElementFragment.setSubquery(subQuery);
        if (null != ctx.order_by_clause()) {
            visit(ctx.order_by_clause());
            OrderByElementsFragment orderByClause = (OrderByElementsFragment) treeBuilder.popStatement();
            factoringElementFragment.setOrderByClause(orderByClause);
        }
        if (null != ctx.search_clause()) {
            visit(ctx.search_clause());
            // SqlStatement searchClause = (SqlStatement) treeBuilder.popStatement();
            // factoringElementFragment.setSearchClause(searchClause);
        }
        if (null != ctx.cycle_clause()) {
            visit(ctx.cycle_clause());
            // SqlStatement cycleClause = (SqlStatement) treeBuilder.popStatement();
            //  factoringElementFragment.setCycleClause(cycleClause);
        }
        treeBuilder.pushStatement(factoringElementFragment);
        return factoringElementFragment;
    }

    /**
     * id
     * : (INTRODUCER char_set_name)? id_expression
     *
     * @param ctx
     * @return
     */
    @Override
    public IdFragment visitQuery_name(PlsqlParser.Query_nameContext ctx) {
        return visitId(ctx.id());
    }

    /**
     * id
     * : (INTRODUCER char_set_name)? id_expression
     *
     * @param ctx
     * @return
     */
    @Override
    public ExpressionStatement visitId_expression(PlsqlParser.Id_expressionContext ctx) {
        List<PlsqlParser.Id_expressionContext> exprs = new ArrayList<>();
        exprs.add(ctx);
        ExpressionStatement expressionStatement = genId_expression(exprs);
        treeBuilder.pushStatement(expressionStatement);
        return expressionStatement;
    }

    /**
     * char_set_name
     * : id_expression ('.' id_expression)*
     *
     * @param ctx
     * @return
     */
    @Override
    public CharSetNameFragment visitChar_set_name(PlsqlParser.Char_set_nameContext ctx) {
        CharSetNameFragment charSetNameFragment = new CharSetNameFragment();
        for (PlsqlParser.Id_expressionContext idExpressionContext : ctx.id_expression()) {
            ExpressionStatement idExpression = visitId_expression(idExpressionContext);
            treeBuilder.popStatement();
            charSetNameFragment.addIdExprssions(idExpression);
        }
        treeBuilder.pushStatement(charSetNameFragment);
        return charSetNameFragment;
    }

    /**
     * id
     * : (INTRODUCER char_set_name)? id_expression
     *
     * @param ctx
     * @return
     */
    @Override
    public IdFragment visitId(PlsqlParser.IdContext ctx) {
        IdFragment idFragment = new IdFragment();
        if (null != ctx.char_set_name()) {
            visit(ctx.char_set_name());
            CharSetNameFragment charSetName = (CharSetNameFragment) treeBuilder.popStatement();
            idFragment.setCharSetName(charSetName);
        }
        ExpressionStatement idExpression = visitId_expression(ctx.id_expression());
        treeBuilder.popStatement();
        idFragment.setIdExpression(idExpression);
        treeBuilder.pushStatement(idFragment);
        return idFragment;
    }

    /**
     * order_by_clause
     * : ORDER SIBLINGS? BY order_by_elements (',' order_by_elements)*
     *
     * @param ctx
     * @return
     */
    @Override
    public OrderByClauseFragment visitOrder_by_clause(PlsqlParser.Order_by_clauseContext ctx) {
        OrderByClauseFragment orderByClause = new OrderByClauseFragment();
        for (PlsqlParser.Order_by_elementsContext order_by_elementsContext : ctx.order_by_elements()) {
            visit(order_by_elementsContext);
            OrderByElementsFragment orderByElement = (OrderByElementsFragment) treeBuilder.popStatement();
            orderByClause.addOrderByElem(orderByElement);
        }
        treeBuilder.pushStatement(orderByClause);
        return orderByClause;
    }

    /**
     * order_by_elements
     * : expression (ASC | DESC)? (NULLS (FIRST | LAST))?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public OrderByElementsFragment visitOrder_by_elements(PlsqlParser.Order_by_elementsContext ctx) {
        OrderByElementsFragment orderByElementsFragment = new OrderByElementsFragment();
        visit(ctx.expression());
        ExpressionStatement es = (ExpressionStatement) treeBuilder.popStatement();
        orderByElementsFragment.setExpression(es);
        if (null != ctx.ASC()) {
            orderByElementsFragment.setOrderType("asc");
        }
        if (null != ctx.DESC()) {
            orderByElementsFragment.setOrderType("desc");
        }
        if (null != ctx.NULLS() && null != ctx.FIRST()) {
            orderByElementsFragment.setNullOrderType("first");
        }
        if (null != ctx.NULLS() && null != ctx.LAST()) {
            orderByElementsFragment.setNullOrderType("last");
        }
        treeBuilder.pushStatement(orderByElementsFragment);
        return orderByElementsFragment;
    }

    /**
     * subquery
     * : subquery_basic_elements subquery_operation_part*
     *
     * @param ctx
     * @return
     */
    @Override
    public SubqueryFragment visitSubquery(PlsqlParser.SubqueryContext ctx) {
        SubqueryFragment subqueryFragment = new SubqueryFragment();
        visit(ctx.subquery_basic_elements());
        SubQuBaseElemFragment basicStmt = (SubQuBaseElemFragment) treeBuilder.popStatement();
        subqueryFragment.setBasicElement(basicStmt);
        for (PlsqlParser.Subquery_operation_partContext opCtx : ctx.subquery_operation_part()) {
            visit(opCtx);
            SubqueryOpPartFragment opStmt = (SubqueryOpPartFragment) treeBuilder.popStatement();
            subqueryFragment.addOperation(opStmt);
        }
        treeBuilder.pushStatement(subqueryFragment);
        return subqueryFragment;
    }

    /**
     * subquery_operation_part
     * : (UNION ALL? | INTERSECT | MINUS) subquery_basic_elements
     *
     * @param ctx
     * @return
     */
    @Override
    public SubqueryOpPartFragment visitSubquery_operation_part(PlsqlParser.Subquery_operation_partContext ctx) {
        SubqueryOpPartFragment subqueryOpPartFragment = new SubqueryOpPartFragment();
        if (null != ctx.UNION()) {
            subqueryOpPartFragment.setOp("UNION");
        }
        if (null != ctx.MINUS()) {
            subqueryOpPartFragment.setOp("MINUS");
        }
        if (null != ctx.INTERSECT()) {
            subqueryOpPartFragment.setOp("INTERSECT");
        }
        visit(ctx.subquery_basic_elements());
        SubQuBaseElemFragment subqueryBaseElement = (SubQuBaseElemFragment) treeBuilder.popStatement();
        subqueryOpPartFragment.setSubQuBaseElemFragment(subqueryBaseElement);
        treeBuilder.pushStatement(subqueryOpPartFragment);
        return subqueryOpPartFragment;
    }

    /**
     * subquery_basic_elements
     * : query_block
     * | '(' subquery ')'
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitSubquery_basic_elements(PlsqlParser.Subquery_basic_elementsContext ctx) {
        SubQuBaseElemFragment subQuBaseElemFragment = new SubQuBaseElemFragment();
        if (null != ctx.query_block()) {
            visit(ctx.query_block());
            QueryBlockFragment queryBlcok = (QueryBlockFragment) treeBuilder.popStatement();
            subQuBaseElemFragment.setQueryBlock(queryBlcok);
        }
        if (null != ctx.subquery()) {
            visit(ctx.subquery());
            SubqueryFragment subQuery = (SubqueryFragment) treeBuilder.popStatement();
            subQuBaseElemFragment.setSubQuery(subQuery);
        }
        treeBuilder.pushStatement(subQuBaseElemFragment);
        return subQuBaseElemFragment;
    }

    /**
     * query_block
     * : SELECT (DISTINCT | UNIQUE | ALL)? ('*' | selected_element (',' selected_element)*)
     * into_clause? from_clause where_clause? hierarchical_query_clause? group_by_clause? model_clause?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitQuery_block(PlsqlParser.Query_blockContext ctx) {
        QueryBlockFragment queryBlockFragment = new QueryBlockFragment();
        if (null != ctx.DISTINCT()) {
            queryBlockFragment.setQuereyType("distinct");
        }
        if (null != ctx.UNIQUE()) {
            queryBlockFragment.setQuereyType("unique");
        }
        if (null != ctx.ALL()) {
            queryBlockFragment.setQuereyType("all");
        }
        for (PlsqlParser.Selected_elementContext eleCtx : ctx.selected_element()) {
            visit(eleCtx);
            SelectElementFragment element = (SelectElementFragment) treeBuilder.popStatement();
            queryBlockFragment.addElement(element);
        }
        if (null != ctx.into_clause()) {
            visit(ctx.into_clause());
            IntoClauseFragment intoClauseFragment = (IntoClauseFragment) treeBuilder.popStatement();
            queryBlockFragment.setIntoClause(intoClauseFragment);
        }
        visit(ctx.from_clause());
        FromClauseFragment fromFrag = (FromClauseFragment) treeBuilder.popStatement();
        queryBlockFragment.setFromClause(fromFrag);
        if (null != ctx.where_clause()) {
            visit(ctx.where_clause());
            WhereClauseFragment whereFrag = (WhereClauseFragment) treeBuilder.popStatement();
            queryBlockFragment.setWhereClause(whereFrag);
        }
        if (null != ctx.hierarchical_query_clause()) {
            treeBuilder.addException("hierarchical_query_clause ", ctx.hierarchical_query_clause());
        }
        if (null != ctx.group_by_clause()) {
            visit(ctx.group_by_clause());
            GroupByFragment groupByFragment = (GroupByFragment) treeBuilder.popStatement();
            queryBlockFragment.setGroupClause(groupByFragment);
        }
        if (null != ctx.model_clause()) {
            treeBuilder.addException("model clause ", ctx.model_clause());
        }
        treeBuilder.pushStatement(queryBlockFragment);
        return queryBlockFragment;
    }


    /**
     * group_by_clause
     * : GROUP BY group_by_elements (',' group_by_elements)* having_clause?
     * | having_clause (GROUP BY group_by_elements (',' group_by_elements)*)?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public GroupByFragment visitGroup_by_clause(PlsqlParser.Group_by_clauseContext ctx) {
        GroupByFragment groupByFragment = new GroupByFragment();
        for (PlsqlParser.Group_by_elementsContext gbec : ctx.group_by_elements()) {
            visit(gbec);
            GroupByElemFragment groupByElemFragment = (GroupByElemFragment) treeBuilder.popStatement();
            groupByFragment.addGroupByElem(groupByElemFragment);
        }
        if (null != ctx.having_clause()) {
            visit(ctx.having_clause());
            HavingClauseFragment havingClauseFragment = (HavingClauseFragment) treeBuilder.popStatement();
            groupByFragment.setHavingClauseFragment(havingClauseFragment);
        }
        treeBuilder.pushStatement(groupByFragment);
        return groupByFragment;
    }

    /**
     * group_by_elements
     * : grouping_sets_clause
     * | rollup_cube_clause
     * | expression
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public GroupByElemFragment visitGroup_by_elements(PlsqlParser.Group_by_elementsContext ctx) {
        GroupByElemFragment groupByElemFragment = new GroupByElemFragment();
        if (null != ctx.expression()) {
            visit(ctx.expression());
            ExpressionStatement expressionStatement = (ExpressionStatement) treeBuilder.popStatement();
            groupByElemFragment.setExpressionStatement(expressionStatement);
        }
        if (null != ctx.grouping_sets_clause()) {
            //TODO
        }
        if (null != ctx.rollup_cube_clause()) {
            //TODO
        }
        treeBuilder.pushStatement(groupByElemFragment);
        return groupByElemFragment;
    }

    /**
     * having_clause
     * : HAVING condition
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public HavingClauseFragment visitHaving_clause(PlsqlParser.Having_clauseContext ctx) {
        HavingClauseFragment havingClauseFragment = new HavingClauseFragment();
        visit(ctx.condition());
        ExpressionStatement expressionStatement = (ExpressionStatement) treeBuilder.popStatement();
        havingClauseFragment.setExpressionStatement(expressionStatement);
        treeBuilder.pushStatement(expressionStatement);
        return havingClauseFragment;
    }

    /**
     * selected_element
     * : select_list_elements column_alias?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public SelectElementFragment visitSelected_element(PlsqlParser.Selected_elementContext ctx) {
        SelectElementFragment elementFragment = new SelectElementFragment();
        visit(ctx.select_list_elements());
        SelectListElementsFragment col = (SelectListElementsFragment) treeBuilder.popStatement();
        elementFragment.setCol(col);
        if (ctx.column_alias() != null) {
            visit(ctx.column_alias());
            ColumnAliasFragment colAlias = (ColumnAliasFragment) treeBuilder.popStatement();
            elementFragment.setColAlias(colAlias);
        }
        treeBuilder.pushStatement(elementFragment);
        return elementFragment;
    }

    /**
     * select_list_elements
     * : tableview_name '.' '*'
     * | expression
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public SelectListElementsFragment visitSelect_list_elements(PlsqlParser.Select_list_elementsContext ctx) {
        SelectListElementsFragment selectListElementsFragment = new SelectListElementsFragment();
        if (ctx.expression() != null) {
            visit(ctx.expression());
            ExpressionStatement es = (ExpressionStatement) treeBuilder.popStatement();
            selectListElementsFragment.setExpression(es);
        }
        if (ctx.tableview_name() != null) {
            visit(ctx.tableview_name());
            TableViewNameFragment tableViewNameFragment = (TableViewNameFragment) treeBuilder.popStatement();
            selectListElementsFragment.setTableViewNameFragment(tableViewNameFragment);
        }
        treeBuilder.pushStatement(selectListElementsFragment);
        return selectListElementsFragment;
    }

    /**
     * tableview_name
     * : id ('.' id_expression)?
     * ('@' link_name | /* partition_extension_clause)?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public TableViewNameFragment visitTableview_name(PlsqlParser.Tableview_nameContext ctx) {
        TableViewNameFragment tableViewNameFragment = new TableViewNameFragment();
        visitId(ctx.id());
        IdFragment id = (IdFragment) treeBuilder.popStatement();
        tableViewNameFragment.setIdFragment(id);
        if (null != ctx.id_expression()) {
            ExpressionStatement idExpression = visitId_expression(ctx.id_expression());
            treeBuilder.popStatement();
            tableViewNameFragment.setIdExpression(idExpression);
        }
        if (null != ctx.link_name()) {
            treeBuilder.addException("visit remote database", ctx.link_name());
        }
        treeBuilder.pushStatement(tableViewNameFragment);
        return tableViewNameFragment;
    }

    /**
     * column_alias
     * : AS? (id | alias_quoted_string)
     * | AS
     *
     * @param ctx
     * @return
     */
    @Override
    public ColumnAliasFragment visitColumn_alias(PlsqlParser.Column_aliasContext ctx) {
        ColumnAliasFragment columnAliasFragment = new ColumnAliasFragment();
        if (null != ctx.id()) {
            visitId(ctx.id());
            IdFragment id = (IdFragment) treeBuilder.popStatement();
            columnAliasFragment.setIdFragment(id);
        }
        if (null != ctx.alias_quoted_string()) {
            String alias = getFullSql(ctx.alias_quoted_string());
            columnAliasFragment.setAlias(alias);
        }
        treeBuilder.pushStatement(columnAliasFragment);
        return columnAliasFragment;
    }

    /**
     * from_clause
     * : FROM table_ref_list
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitFrom_clause(PlsqlParser.From_clauseContext ctx) {
        FromClauseFragment fromClauseFragment = new FromClauseFragment();
        visit(ctx.table_ref_list());
        TableRefListFragment sourceStmt = (TableRefListFragment) treeBuilder.popStatement();
        fromClauseFragment.setSourceFrag(sourceStmt);
        treeBuilder.pushStatement(fromClauseFragment);
        return fromClauseFragment;
    }

    /**
     * table_ref_list
     * : table_ref (',' table_ref)*
     *
     * @param ctx
     * @return
     */
    @Override
    public TableRefListFragment visitTable_ref_list(PlsqlParser.Table_ref_listContext ctx) {
        TableRefListFragment tableRefListFragment = new TableRefListFragment();
        for (PlsqlParser.Table_refContext refCtx : ctx.table_ref()) {
            visit(refCtx);
            TableRefFragment stmt = (TableRefFragment) treeBuilder.popStatement();
            tableRefListFragment.addFragment(stmt);
        }
        treeBuilder.pushStatement(tableRefListFragment);
        return tableRefListFragment;
    }

    /**
     * table_ref
     * : table_ref_aux join_clause* (pivot_clause | unpivot_clause)?
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitTable_ref(PlsqlParser.Table_refContext ctx) {
        TableRefFragment tableRefFragment = new TableRefFragment();
        visit(ctx.table_ref_aux());
        TableRefAuxFragment refStmt = (TableRefAuxFragment) treeBuilder.popStatement();
        tableRefFragment.setTableRefAuxFragment(refStmt);
        for (PlsqlParser.Join_clauseContext joinCtx : ctx.join_clause()) {
            visit(joinCtx);
            JoinClauseFragment joinClauseFragment = (JoinClauseFragment) treeBuilder.popStatement();
            tableRefFragment.addJoinClauseFragment(joinClauseFragment);
        }
        if (null != ctx.pivot_clause()) {
            visit(ctx.pivot_clause());
            PivotClauseFragment pivotClauseFragment = (PivotClauseFragment) treeBuilder.popStatement();
            tableRefFragment.setPivotClauseFragment(pivotClauseFragment);
        }
        if (null != ctx.unpivot_clause()) {
            visit(ctx.unpivot_clause());
            UnpivotClauseFragment unpivotClauseFragment = (UnpivotClauseFragment) treeBuilder.popStatement();
            tableRefFragment.setUnpivotClauseFragment(unpivotClauseFragment);
        }
        treeBuilder.pushStatement(tableRefFragment);
        return tableRefFragment;
    }

    /**
     * pivot_clause
     * : PIVOT XML? '(' pivot_element (',' pivot_element)* pivot_for_clause pivot_in_clause ')'
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitPivot_clause(PlsqlParser.Pivot_clauseContext ctx) {
        PivotClauseFragment pivotClauseFragment = new PivotClauseFragment();
        if (null != ctx.XML()) {
            pivotClauseFragment.setXml("xml");
        }
        for (PlsqlParser.Pivot_elementContext pec : ctx.pivot_element()) {
            visit(pec);
            PivotElementFragment pef = (PivotElementFragment) treeBuilder.popStatement();
            pivotClauseFragment.addPivotElement(pef);
        }
        visit(ctx.pivot_for_clause());
        PivotForClauseFragment pfcf = (PivotForClauseFragment) treeBuilder.popStatement();
        pivotClauseFragment.setPivotForClauseFragment(pfcf);
        visit(ctx.pivot_in_clause());
        PivotInClauseFragment picf = (PivotInClauseFragment) treeBuilder.popStatement();
        pivotClauseFragment.setPivotInClauseFragment(picf);
        treeBuilder.pushStatement(pivotClauseFragment);
        return pivotClauseFragment;
    }

    /**
     * pivot_in_clause
     * : IN '(' (subquery | ANY (',' ANY)* | pivot_in_clause_element (',' pivot_in_clause_element)*) ')'
     * ;
     * <p>
     * pivot_in_clause_element
     * : pivot_in_clause_elements column_alias?
     * ;
     * <p>
     * pivot_in_clause_elements
     * : expression
     * | expression_list
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitPivot_in_clause(PlsqlParser.Pivot_in_clauseContext ctx) {
        PivotInClauseFragment pivotInClauseFragment = new PivotInClauseFragment();
        if (null != ctx.subquery()) {
            visit(ctx.subquery());
            SubqueryFragment subqueryFragment = (SubqueryFragment) treeBuilder.popStatement();
            pivotInClauseFragment.setSubqueryFragment(subqueryFragment);
        }
        if (!ctx.ANY().isEmpty()) {
            for (TerminalNode any : ctx.ANY()) {
                pivotInClauseFragment.addAnys(any.getText());
            }
        }
        if (!ctx.pivot_in_clause_element().isEmpty()) {
            for (PlsqlParser.Pivot_in_clause_elementContext pice : ctx.pivot_in_clause_element()) {
                visit(pice);
                PivotInClauseElemetFm pivotInClauseElemetFm = (PivotInClauseElemetFm) treeBuilder.popStatement();
                pivotInClauseFragment.addPivotInClauseFms(pivotInClauseElemetFm);
            }
        }
        treeBuilder.pushStatement(pivotInClauseFragment);
        return pivotInClauseFragment;
    }

    /**
     * pivot_in_clause_element
     * : pivot_in_clause_elements column_alias?
     *
     * @param ctx
     * @return
     */
    @Override
    public PivotInClauseElemetFm visitPivot_in_clause_element(PlsqlParser.Pivot_in_clause_elementContext ctx) {
        PivotInClauseElemetFm pivotInClauseElemetFm = new PivotInClauseElemetFm();
        visit(ctx.pivot_in_clause_elements());
        PivotInClauseElemetsFm fm = (PivotInClauseElemetsFm) treeBuilder.popStatement();
        pivotInClauseElemetFm.setPivotInClauseElemetsFm(fm);
        if (null != ctx.column_alias()) {
            visit(ctx.column_alias());
            ColumnAliasFragment columnAliasFragment = (ColumnAliasFragment) treeBuilder.popStatement();
            pivotInClauseElemetFm.setColumnAliasFragment(columnAliasFragment);
        }
        treeBuilder.pushStatement(pivotInClauseElemetFm);
        return pivotInClauseElemetFm;
    }

    /**
     * pivot_in_clause_elements
     * : expression
     * | expression_list
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitPivot_in_clause_elements(PlsqlParser.Pivot_in_clause_elementsContext ctx) {
        PivotInClauseElemetsFm pivotInClauseElemetsFm = new PivotInClauseElemetsFm();
        if (null != ctx.expression()) {
            visit(ctx.expression());
            ExpressionStatement es = (ExpressionStatement) treeBuilder.popStatement();
            pivotInClauseElemetsFm.setExpressionStatement(es);
        }
        if (null != ctx.expression_list()) {
            visit(ctx.expression_list());
            ExpressionListFragment es = (ExpressionListFragment) treeBuilder.popStatement();
            pivotInClauseElemetsFm.setExpressionListFragments(es);
        }
        treeBuilder.pushStatement(pivotInClauseElemetsFm);
        return pivotInClauseElemetsFm;
    }

    /**
     * pivot_for_clause
     * : FOR (column_name | '(' column_name (',' column_name)* ')')
     *
     * @param ctx
     * @return
     */
    @Override
    public PivotForClauseFragment visitPivot_for_clause(PlsqlParser.Pivot_for_clauseContext ctx) {
        PivotForClauseFragment pivotForClauseFragment = new PivotForClauseFragment();
        for (PlsqlParser.Column_nameContext cnc : ctx.column_name()) {
            visit(cnc);
            ColumnNameFragment columnNameFragment = (ColumnNameFragment) treeBuilder.popStatement();
            pivotForClauseFragment.addColumn(columnNameFragment);
        }
        treeBuilder.pushStatement(pivotForClauseFragment);
        return pivotForClauseFragment;
    }

    /**
     * pivot_element
     * : aggregate_function_name '(' expression ')' column_alias?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public PivotElementFragment visitPivot_element(PlsqlParser.Pivot_elementContext ctx) {
        PivotElementFragment pivotElementFragment = new PivotElementFragment();
        visit(ctx.aggregate_function_name());
        AggregateFunctionNameFm afnf = (AggregateFunctionNameFm) treeBuilder.popStatement();
        pivotElementFragment.setAggregateFunctionNameFm(afnf);
        visit(ctx.expression());
        ExpressionStatement expressionStatement = (ExpressionStatement) treeBuilder.popStatement();
        pivotElementFragment.setExpressionStatement(expressionStatement);
        visit(ctx.column_alias());
        ColumnAliasFragment columnAliasFragment = (ColumnAliasFragment) treeBuilder.popStatement();
        pivotElementFragment.setColumnAliasFragment(columnAliasFragment);
        treeBuilder.pushStatement(pivotElementFragment);
        return pivotElementFragment;
    }

    /**
     * aggregate_function_name
     * : id ('.' id_expression)*
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public AggregateFunctionNameFm visitAggregate_function_name(PlsqlParser.Aggregate_function_nameContext ctx) {
        AggregateFunctionNameFm afnf = new AggregateFunctionNameFm();
        visit(ctx.id());
        IdFragment idFragment = (IdFragment) treeBuilder.popStatement();
        afnf.setIdFragment(idFragment);
        List<String> idExpressions = new ArrayList<>();
        for (PlsqlParser.Id_expressionContext ide : ctx.id_expression()) {
            ExpressionStatement idExpression = visitId_expression(ide);
            treeBuilder.popStatement();
            afnf.addExpression(idExpression);
        }
        treeBuilder.pushStatement(afnf);
        return afnf;
    }

    /**
     * unpivot_clause
     * : UNPIVOT ((INCLUDE | EXCLUDE) NULLS)?
     * '(' (column_name | '(' column_name (',' column_name)* ')') pivot_for_clause unpivot_in_clause ')'
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public UnpivotClauseFragment visitUnpivot_clause(PlsqlParser.Unpivot_clauseContext ctx) {
        UnpivotClauseFragment unpivotClauseFragment = new UnpivotClauseFragment();
        if (null != ctx.INCLUDE()) {
            unpivotClauseFragment.setInclude("include nulls");
        }
        if (null != ctx.EXCLUDE()) {
            unpivotClauseFragment.setInclude("exclude nulls");
        }
        for (PlsqlParser.Column_nameContext cnc : ctx.column_name()) {
            visit(cnc);
            ColumnNameFragment columnNameFragment = (ColumnNameFragment) treeBuilder.popStatement();
            unpivotClauseFragment.addColumnNames(columnNameFragment);
        }
        visit(ctx.pivot_for_clause());
        PivotForClauseFragment pfcf = (PivotForClauseFragment) treeBuilder.popStatement();
        unpivotClauseFragment.setPivotForClauseFragment(pfcf);
        visit(ctx.unpivot_in_clause());
        UnpivotInClauseFm unpivotInClauseFm = (UnpivotInClauseFm) treeBuilder.popStatement();
        unpivotClauseFragment.setUnpivotInClauseFm(unpivotInClauseFm);
        treeBuilder.pushStatement(unpivotClauseFragment);
        return unpivotClauseFragment;
    }

    /**
     * unpivot_in_clause
     * : IN '(' unpivot_in_elements (',' unpivot_in_elements)* ')'
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public UnpivotInClauseFm visitUnpivot_in_clause(PlsqlParser.Unpivot_in_clauseContext ctx) {
        UnpivotInClauseFm unpivotInClauseFm = new UnpivotInClauseFm();
        for (PlsqlParser.Unpivot_in_elementsContext uiec : ctx.unpivot_in_elements()) {
            visit(uiec);
            UnpivotInElementsFm unpivotInElementsFm = (UnpivotInElementsFm) treeBuilder.popStatement();
            unpivotInClauseFm.addUnpivot(unpivotInElementsFm);
        }
        treeBuilder.pushStatement(unpivotInClauseFm);
        return unpivotInClauseFm;
    }

    /**
     * unpivot_in_elements
     * : (column_name | '(' column_name (',' column_name)* ')')
     * (AS (constant | '(' constant (',' constant)* ')'))?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public UnpivotInElementsFm visitUnpivot_in_elements(PlsqlParser.Unpivot_in_elementsContext ctx) {
        UnpivotInElementsFm unpivotInElementsFm = new UnpivotInElementsFm();
        for (PlsqlParser.Column_nameContext cnc : ctx.column_name()) {
            visit(cnc);
            ColumnNameFragment columnNameFragment = (ColumnNameFragment) treeBuilder.popStatement();
            unpivotInElementsFm.addColumnNames(columnNameFragment);
        }
        for (PlsqlParser.ConstantContext cc : ctx.constant()) {
            visit(cc);
            ExpressionStatement es = (ExpressionStatement) treeBuilder.popStatement();
            unpivotInElementsFm.addConstants(es);
        }
        treeBuilder.pushStatement(unpivotInElementsFm);
        return unpivotInElementsFm;
    }

    /**
     * outer_join_type
     * : (FULL | LEFT | RIGHT) OUTER?
     *
     * @param ctx
     * @return
     */
    @Override
    public String visitOuter_join_type(PlsqlParser.Outer_join_typeContext ctx) {
        StringBuffer sql = new StringBuffer();
        if (null != ctx.FULL()) {
            sql.append(" full");
        }
        if (null != ctx.LEFT()) {
            sql.append(" left");
        }
        if (null != ctx.RIGHT()) {
            sql.append(" right");
        }
        if (null != ctx.OUTER()) {
            sql.append(" outer ");
        }
        return sql.toString();
    }

    /**
     * join_clause
     * : query_partition_clause? (CROSS | NATURAL)? (INNER | outer_join_type)?
     * JOIN table_ref_aux query_partition_clause? (join_on_part | join_using_part)*
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitJoin_clause(PlsqlParser.Join_clauseContext ctx) {
        JoinClauseFragment joinClauseFragment = new JoinClauseFragment();
        if (null != ctx.leftquery) {
            visit(ctx.leftquery);
            QueryPartitionClauseFragement queryPartitionClauseFragement =
                    (QueryPartitionClauseFragement) treeBuilder.popStatement();
            joinClauseFragment.setLeftQueryPartitionClauseFragements(queryPartitionClauseFragement);
        }
        if (null != ctx.rightquery) {
            visit(ctx.rightquery);
            QueryPartitionClauseFragement queryPartitionClauseFragement =
                    (QueryPartitionClauseFragement) treeBuilder.popStatement();
            joinClauseFragment.setRightQueryPartitionClauseFragements(queryPartitionClauseFragement);
        }
        if (null != ctx.CROSS()) {
            joinClauseFragment.setCorssJoninType("cross");
        }
        if (null != ctx.NATURAL()) {
            joinClauseFragment.setCorssJoninType("natural");
        }
        if (null != ctx.INNER()) {
            joinClauseFragment.setOutJoinType("inner join ");
        }
        if (null != ctx.outer_join_type()) {
            String joinStr = visitOuter_join_type(ctx.outer_join_type()) + " join ";
            joinClauseFragment.setOutJoinType(joinStr);
        }
        visit(ctx.table_ref_aux());
        TableRefAuxFragment tableRefAuxFragment = (TableRefAuxFragment) treeBuilder.popStatement();
        joinClauseFragment.setTableRefAuxFragment(tableRefAuxFragment);
        for (PlsqlParser.Join_on_partContext jopc : ctx.join_on_part()) {
            visit(jopc);
            JoinOnPartFragment joinOnPartFragment = (JoinOnPartFragment) treeBuilder.popStatement();
            joinClauseFragment.addJoinOnPart(joinOnPartFragment);
        }
        for (PlsqlParser.Join_using_partContext jupc : ctx.join_using_part()) {
            visit(jupc);
            JoinUsingPartFragment joinUsingPartFragment = (JoinUsingPartFragment) treeBuilder.popStatement();
            joinClauseFragment.addJoinUsingPart(joinUsingPartFragment);
        }
        treeBuilder.pushStatement(joinClauseFragment);
        return joinClauseFragment;
    }

    /**
     * join_using_part
     * : USING '(' column_name (',' column_name)* ')'
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public JoinUsingPartFragment visitJoin_using_part(PlsqlParser.Join_using_partContext ctx) {
        JoinUsingPartFragment joinUsingPartFragment = new JoinUsingPartFragment();
        for (PlsqlParser.Column_nameContext column_nameContext : ctx.column_name()) {
            visit(column_nameContext);
            ColumnNameFragment columnNameFragment = (ColumnNameFragment) treeBuilder.popStatement();
            joinUsingPartFragment.addColumns(columnNameFragment);
        }
        treeBuilder.pushStatement(joinUsingPartFragment);
        return joinUsingPartFragment;
    }

    /**
     * query_partition_clause
     * : PARTITION BY ('(' subquery ')' | expression_list | expression (',' expression)*)
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitQuery_partition_clause(PlsqlParser.Query_partition_clauseContext ctx) {
        QueryPartitionClauseFragement queryPartitionClauseFragement = new QueryPartitionClauseFragement();
        treeBuilder.pushStatement(queryPartitionClauseFragement);
        return queryPartitionClauseFragement;
    }

    /**
     * table_ref_aux
     * : (dml_table_expression_clause (pivot_clause | unpivot_clause)?
     * | '(' table_ref subquery_operation_part* ')' (pivot_clause | unpivot_clause)?
     * | ONLY '(' dml_table_expression_clause ')'
     * | dml_table_expression_clause (pivot_clause | unpivot_clause)?)
     * flashback_query_clause* (table_alias)?
     *
     * @param ctx
     * @return
     */
    @Override
    public TableRefAuxFragment visitTable_ref_aux(PlsqlParser.Table_ref_auxContext ctx) {
        TableRefAuxFragment tableRefAuxFragment = new TableRefAuxFragment();
        if (ctx.dml_table_expression_clause() != null) {
            visit(ctx.dml_table_expression_clause());
            DmlTableExpressionFragment dmlTableExpressionFragment = (DmlTableExpressionFragment) treeBuilder.popStatement();
            tableRefAuxFragment.setDmlTableExpressionFragment(dmlTableExpressionFragment);
            if (ctx.ONLY() == null) {
                if (null != ctx.pivot_clause()) {
                    visit(ctx.pivot_clause());
                    PivotClauseFragment pivotClauseFragment = (PivotClauseFragment) treeBuilder.popStatement();
                    tableRefAuxFragment.setPivotClauseFragment(pivotClauseFragment);
                }
                if (null != ctx.unpivot_clause()) {
                    visit(ctx.unpivot_clause());
                    UnpivotClauseFragment unpivotClauseFragment = (UnpivotClauseFragment) treeBuilder.popStatement();
                    tableRefAuxFragment.setUnpivotClauseFragment(unpivotClauseFragment);
                }
            } else {
                tableRefAuxFragment.setOnly("only");
            }
        }
        /**
         * flashback_query_clause 允许 DBA 看到特定时间的列值
         */
        if (!ctx.flashback_query_clause().isEmpty()) {
            treeBuilder.addException("flash back query ", ctx.flashback_query_clause(0));
        }
        if (null != ctx.table_alias()) {
            visit(ctx.table_alias());
            TableAliasFragment tableAliasFragment = (TableAliasFragment) treeBuilder.popStatement();
            tableRefAuxFragment.setTableAliasFragment(tableAliasFragment);
        }
        treeBuilder.pushStatement(tableRefAuxFragment);
        return tableRefAuxFragment;
    }

    /**
     * dml_table_expression_clause
     * : table_collection_expression
     * | '(' select_statement subquery_restriction_clause? ')'
     * | tableview_name sample_clause?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public DmlTableExpressionFragment visitDml_table_expression_clause(PlsqlParser.Dml_table_expression_clauseContext ctx) {
        DmlTableExpressionFragment dmlTableExpressionFragment = new DmlTableExpressionFragment();
        if (null != ctx.table_collection_expression()) {
            visit(ctx.table_collection_expression());
            TableCollectionExpressionFm tableCollectionExpressionFm = (TableCollectionExpressionFm) treeBuilder.popStatement();
            dmlTableExpressionFragment.setTableCollectionExpressionFm(tableCollectionExpressionFm);
        }
        if (null != ctx.select_statement()) {
            visit(ctx.select_statement());
            OracleSelectStatement oracleSelectStatement = (OracleSelectStatement) treeBuilder.popStatement();
            dmlTableExpressionFragment.setSelectStatement(oracleSelectStatement);
        }
        if (null != ctx.subquery_restriction_clause()) {
            treeBuilder.addException("subquery restriction query ", ctx.subquery_restriction_clause());
        }
        if (null != ctx.tableview_name()) {
            visit(ctx.tableview_name());
            TableViewNameFragment tableViewNameFragment = (TableViewNameFragment) treeBuilder.popStatement();
            dmlTableExpressionFragment.setTableViewNameFragment(tableViewNameFragment);
        }
        if (null != ctx.sample_clause()) {
            treeBuilder.addException("random sampling ", ctx.sample_clause());
        }
        treeBuilder.pushStatement(dmlTableExpressionFragment);
        return dmlTableExpressionFragment;
    }

    /**
     * table_collection_expression
     * : (TABLE | THE) ('(' subquery ')' | '(' expression ')' ('(' '+' ')')?)
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public TableCollectionExpressionFm visitTable_collection_expression(PlsqlParser.Table_collection_expressionContext ctx) {
        TableCollectionExpressionFm tableCollectionExpressionFm = new TableCollectionExpressionFm();
        if (null != ctx.TABLE()) {
            tableCollectionExpressionFm.setKeyWord("table");
        }
        if (null != ctx.THE()) {
            tableCollectionExpressionFm.setKeyWord("the");
        }
        visit(ctx.subquery());
        SubqueryFragment subqueryFragment = (SubqueryFragment) treeBuilder.popStatement();
        tableCollectionExpressionFm.setSubqueryFragment(subqueryFragment);
        visit(ctx.expression());
        ExpressionStatement expressionStatement = (ExpressionStatement) treeBuilder.popStatement();
        tableCollectionExpressionFm.setExpressionStatement(expressionStatement);
        treeBuilder.pushStatement(tableCollectionExpressionFm);
        return tableCollectionExpressionFm;
    }

    /**
     * join_on_part
     * : ON condition
     *
     * @param ctx
     * @return
     */
    @Override
    public JoinOnPartFragment visitJoin_on_part(PlsqlParser.Join_on_partContext ctx) {
        JoinOnPartFragment joinOnPartFragment = new JoinOnPartFragment();
        visitCondition(ctx.condition());
        ExpressionStatement expressionStatement = (ExpressionStatement) treeBuilder.popStatement();
        joinOnPartFragment.setEs(expressionStatement);
        treeBuilder.pushStatement(joinOnPartFragment);
        return joinOnPartFragment;
    }

    /**
     * condition
     * : expression
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public ExpressionStatement visitCondition(PlsqlParser.ConditionContext ctx) {
        visit(ctx.expression());
        ExpressionStatement expressionStatement = (ExpressionStatement) treeBuilder.popStatement();
        treeBuilder.pushStatement(expressionStatement);
        return expressionStatement;
    }

    /**
     * where_clause
     * : WHERE (current_of_clause | expression)
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitWhere_clause(PlsqlParser.Where_clauseContext ctx) {
        WhereClauseFragment whereClauseFragment = new WhereClauseFragment();
        if (ctx.expression() != null) {
            visit(ctx.expression());
            SqlStatement exprStmt = (SqlStatement) treeBuilder.popStatement();
            whereClauseFragment.setCondition(exprStmt);
        }
        if (null != ctx.current_of_clause()) {
            //TODO cursor_name
        }
        treeBuilder.pushStatement(whereClauseFragment);
        return whereClauseFragment;
    }

    private SqlStatement genSqlStringFragment(String str) {
        SqlStatement sqlStatement = new SqlStatement();
        sqlStatement.setSql(str);
        return sqlStatement;
    }

    private String getFullSql(ParserRuleContext ctx) {
        return ctx.start.getInputStream().getText(
                new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
    }

    private Position locate(ParserRuleContext ctx) {
        return new Position(ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine(),
                ctx.getStop().getLine(), ctx.getStop().getCharPositionInLine());
    }

    @Override
    public Object visitCursor_declaration(PlsqlParser.Cursor_declarationContext ctx) {
        DeclareCursorStatement stmt = new DeclareCursorStatement();
        OracleCursor cursor = new OracleCursor(ctx.cursor_name().getText());
        // TODO cursor return Type
        // TODO cursor parameter
        for (PlsqlParser.Parameter_specContext para : ctx.parameter_spec()) {
            visit(para);
            treeBuilder.popStatement();
        }
        if (ctx.select_statement() != null) {
            visit(ctx.select_statement());
            cursor.setTreeNode(treeBuilder.popStatement());
        }
        stmt.setCursor(cursor);
        treeBuilder.pushStatement(stmt);
        return stmt;
    }

    @Override
    public Object visitClose_statement(PlsqlParser.Close_statementContext ctx) {
        OracleCloseCursorStmt closeCursorStmt = new OracleCloseCursorStmt(ctx.cursor_name().getText(), false);
        treeBuilder.pushStatement(closeCursorStmt);
        return closeCursorStmt;
    }

    @Override
    public Object visitOpen_statement(PlsqlParser.Open_statementContext ctx) {
        OracleOpenCursorStmt openCursorStmt = new OracleOpenCursorStmt(ctx.cursor_name().getText(), false);
        // TODO open expression list
        treeBuilder.pushStatement(openCursorStmt);
        return openCursorStmt;
    }

    @Override
    public Object visitFetch_statement(PlsqlParser.Fetch_statementContext ctx) {
        OracleFetchCursorStmt fetchCursorStmt = new OracleFetchCursorStmt(ctx.cursor_name().getText());
        for (PlsqlParser.Variable_nameContext varNameCtx : ctx.variable_name()) {
            String varName = varNameCtx.getText();
            fetchCursorStmt.addVarname(varName);
        }
        if (ctx.BULK() != null) {
            fetchCursorStmt.setBulkCollect();
        }
        treeBuilder.pushStatement(fetchCursorStmt);
        return fetchCursorStmt;
    }

    @Override
    public Object visitOpen_for_statement(PlsqlParser.Open_for_statementContext ctx) {
        OracleOpenCursorStmt openCursorStmt = new OracleOpenCursorStmt();
        treeBuilder.pushStatement(openCursorStmt);
        return openCursorStmt;
    }
    //======================================update===============================

    /**
     * update_statement
     * : UPDATE general_table_ref update_set_clause where_clause? static_returning_clause? error_logging_clause?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public OracleUpdateStatement visitUpdate_statement(PlsqlParser.Update_statementContext ctx) {
        OracleUpdateStatement updateStatement = new OracleUpdateStatement();
        visit(ctx.general_table_ref());
        GeneralTableRefFragment generalTableRefFragment = (GeneralTableRefFragment) treeBuilder.popStatement();
        updateStatement.setGeneralTableRefFragment(generalTableRefFragment);
        visit(ctx.update_set_clause());
        UpdateSetClauseFm updateSetClauseFm = (UpdateSetClauseFm) treeBuilder.popStatement();
        updateStatement.setUpdateSetClauseFm(updateSetClauseFm);
        if (null != ctx.where_clause()) {
            visit(ctx.where_clause());
            WhereClauseFragment whereClauseFragment = (WhereClauseFragment) treeBuilder.popStatement();
            updateStatement.setWhereClauseFragment(whereClauseFragment);
        }
        if (null != ctx.static_returning_clause()) {
            visit(ctx.static_returning_clause());
            StaticReturningClauseFm staticReturningClauseFm = (StaticReturningClauseFm) treeBuilder.popStatement();
            updateStatement.setStaticReturningClauseFm(staticReturningClauseFm);
        }
        if (null != ctx.error_logging_clause()) {
            //TODO
        }
        treeBuilder.pushStatement(updateStatement);
        return updateStatement;
    }

    /**
     * static_returning_clause
     * : (RETURNING | RETURN) expression (',' expression)* into_clause
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public StaticReturningClauseFm visitStatic_returning_clause(PlsqlParser.Static_returning_clauseContext ctx) {
        StaticReturningClauseFm staticReturningClauseFm = new StaticReturningClauseFm();
        for (PlsqlParser.ExpressionContext ex : ctx.expression()) {
            visit(ex);
            ExpressionStatement expressionStatement = (ExpressionStatement) treeBuilder.popStatement();
            staticReturningClauseFm.addExpression(expressionStatement);
        }
        visit(ctx.into_clause());
        IntoClauseFragment intoClauseFragment = (IntoClauseFragment) treeBuilder.popStatement();
        staticReturningClauseFm.setIntoClauseFragment(intoClauseFragment);
        treeBuilder.pushStatement(staticReturningClauseFm);
        return staticReturningClauseFm;
    }

    /**
     * into_clause
     * : INTO variable_name (',' variable_name)*
     * | BULK COLLECT INTO variable_name (',' variable_name)*
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public IntoClauseFragment visitInto_clause(PlsqlParser.Into_clauseContext ctx) {
        IntoClauseFragment intoClauseFragment = new IntoClauseFragment();
        if (null != ctx.BULK()) {
            intoClauseFragment.setBulk("bulk collect into");
        }
        for (PlsqlParser.Variable_nameContext vc : ctx.variable_name()) {
            visit(vc);
            VariableNameFragment vnf = (VariableNameFragment) treeBuilder.popStatement();
            intoClauseFragment.addVariableName(vnf);
        }
        treeBuilder.pushStatement(intoClauseFragment);
        return intoClauseFragment;
    }

    /**
     * variable_name
     * : (INTRODUCER char_set_name)? id_expression ('.' id_expression)?
     * | bind_variable
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public VariableNameFragment visitVariable_name(PlsqlParser.Variable_nameContext ctx) {
        VariableNameFragment variableNameFragment = new VariableNameFragment();
        if (null != ctx.bind_variable()) {
            visit(ctx.bind_variable());
            BindVariableNameFm bindVariableNameFm = (BindVariableNameFm) treeBuilder.popStatement();
            variableNameFragment.setBindVariableNameFm(bindVariableNameFm);
        }
        if (null != ctx.INTRODUCER()) {
            variableNameFragment.setIntroducer("introducer");
        }
        if (null != ctx.char_set_name()) {
            visit(ctx.char_set_name());
            CharSetNameFragment charSetNameFragment = (CharSetNameFragment) treeBuilder.popStatement();
            variableNameFragment.setCharSetNameFragment(charSetNameFragment);
        }
        for (PlsqlParser.Id_expressionContext iec : ctx.id_expression()) {
            ExpressionStatement iecStr = visitId_expression(iec);
            treeBuilder.popStatement();
            variableNameFragment.addIdExpression(iecStr);
        }
        treeBuilder.pushStatement(variableNameFragment);
        return variableNameFragment;
    }

    @Override
    public Object visitBind_variable(PlsqlParser.Bind_variableContext ctx) {
        //TODO BIND_VARIABLE
        return null;
    }

    /**
     * general_table_ref
     * : (dml_table_expression_clause | ONLY '(' dml_table_expression_clause ')') table_alias?
     *
     * @param ctx
     * @return
     */
    @Override
    public GeneralTableRefFragment visitGeneral_table_ref(PlsqlParser.General_table_refContext ctx) {
        GeneralTableRefFragment generalTableRefFragment = new GeneralTableRefFragment();
        visit(ctx.dml_table_expression_clause());
        DmlTableExpressionFragment dmlTableExpressionFragment = (DmlTableExpressionFragment) treeBuilder.popStatement();
        generalTableRefFragment.setDmlTableExpressionFragment(dmlTableExpressionFragment);
        if (null != ctx.ONLY()) {
            generalTableRefFragment.setOnly("only");
        }
        if (null != ctx.table_alias()) {
            visit(ctx.table_alias());
            TableAliasFragment tableAliasFragment = (TableAliasFragment) treeBuilder.popStatement();
            generalTableRefFragment.setTableAliasFragment(tableAliasFragment);
        }
        treeBuilder.pushStatement(generalTableRefFragment);
        return generalTableRefFragment;
    }

    /**
     * table_alias
     * : (id | alias_quoted_string)
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public TableAliasFragment visitTable_alias(PlsqlParser.Table_aliasContext ctx) {
        TableAliasFragment tableAliasFragment = new TableAliasFragment();
        if (null != ctx.id()) {
            visitId(ctx.id());
            IdFragment id = (IdFragment) treeBuilder.popStatement();
            tableAliasFragment.setIdFragment(id);
        }
        if (null != ctx.alias_quoted_string()) {
            String alias = getFullSql(ctx.alias_quoted_string());
            tableAliasFragment.setAlias(alias);
        }
        treeBuilder.pushStatement(tableAliasFragment);
        return tableAliasFragment;
    }

    /**
     * update_set_clause
     * : SET
     * (column_based_update_set_clause (',' column_based_update_set_clause)* | VALUE '(' id ')' '=' expression)
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public UpdateSetClauseFm visitUpdate_set_clause(PlsqlParser.Update_set_clauseContext ctx) {
        UpdateSetClauseFm updateSetClauseFm = new UpdateSetClauseFm();
        for (PlsqlParser.Column_based_update_set_clauseContext cbusc : ctx.column_based_update_set_clause()) {
            visit(cbusc);
            ColumnBasedUpDateFm cbudf = (ColumnBasedUpDateFm) treeBuilder.popStatement();
            updateSetClauseFm.addColumnBaseUpdate(cbudf);
        }
        if (null != ctx.VALUE()) {
            updateSetClauseFm.setValue("value");
        }
        if (null != ctx.id()) {
            visit(ctx.id());
            IdFragment idFragment = (IdFragment) treeBuilder.popStatement();
            updateSetClauseFm.setIdFragment(idFragment);
        }
        if (null != ctx.expression()) {
            visit(ctx.expression());
            ExpressionStatement expressionStatement = (ExpressionStatement) treeBuilder.popStatement();
            updateSetClauseFm.setExpressionStatement(expressionStatement);
        }
        treeBuilder.pushStatement(updateSetClauseFm);
        return updateSetClauseFm;
    }

    /**
     * column_based_update_set_clause
     * : column_name '=' expression
     * | '(' column_name (',' column_name)* ')' '=' subquery
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public ColumnBasedUpDateFm visitColumn_based_update_set_clause(PlsqlParser.Column_based_update_set_clauseContext ctx) {
        ColumnBasedUpDateFm columnBasedUpDateFm = new ColumnBasedUpDateFm();
        for (PlsqlParser.Column_nameContext columnNameContext : ctx.column_name()) {
            visit(columnNameContext);
            ColumnNameFragment column = (ColumnNameFragment) treeBuilder.popStatement();
            columnBasedUpDateFm.addColumnFm(column);
        }
        if (null != ctx.expression()) {
            visit(ctx.expression());
            ExpressionStatement expressionStatement = (ExpressionStatement) treeBuilder.popStatement();
            columnBasedUpDateFm.setExpressionStatement(expressionStatement);
        }
        if (null != ctx.subquery()) {
            visit(ctx.subquery());
            SubqueryFragment subqueryFragment = (SubqueryFragment) treeBuilder.popStatement();
            columnBasedUpDateFm.setSubqueryFragment(subqueryFragment);
        }
        treeBuilder.pushStatement(columnBasedUpDateFm);
        return columnBasedUpDateFm;
    }
    //==========================delete================================================================

    /**
     * delete_statement
     * : DELETE FROM? general_table_ref where_clause? static_returning_clause? error_logging_clause?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public OracleDelStatement visitDelete_statement(PlsqlParser.Delete_statementContext ctx) {
        OracleDelStatement delStatement = new OracleDelStatement();
        visit(ctx.general_table_ref());
        GeneralTableRefFragment generalTableRefFragment = (GeneralTableRefFragment) treeBuilder.popStatement();
        delStatement.setGeneralTableRefFragment(generalTableRefFragment);
        if (null != ctx.where_clause()) {
            visit(ctx.where_clause());
            WhereClauseFragment whereClauseFragment = (WhereClauseFragment) treeBuilder.popStatement();
            delStatement.setWhereClauseFragment(whereClauseFragment);
        }
        if (null != ctx.static_returning_clause()) {
            visit(ctx.static_returning_clause());
            StaticReturningClauseFm staticReturningClauseFm = (StaticReturningClauseFm) treeBuilder.popStatement();
            delStatement.setStaticReturningClauseFm(staticReturningClauseFm);
        }
        if (null != ctx.error_logging_clause()) {
            //TODO ERRORLOG
        }
        treeBuilder.pushStatement(delStatement);
        return delStatement;
    }
    //=========================================insert statement============================

    /**
     * insert_statement
     * : INSERT (single_table_insert | multi_table_insert)
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public OracleInsertStatement visitInsert_statement(PlsqlParser.Insert_statementContext ctx) {
        OracleInsertStatement oracleInsertStatement = new OracleInsertStatement();
        if (null != ctx.single_table_insert()) {
            visit(ctx.single_table_insert());
            SingleTableInsertFragment stif = (SingleTableInsertFragment) treeBuilder.popStatement();
            oracleInsertStatement.setSingleTableInsertFragment(stif);
        }
        if (null != ctx.multi_table_insert()) {
            visit(ctx.multi_table_insert());
            MultiTableInsertFragment mtif = (MultiTableInsertFragment) treeBuilder.popStatement();
            oracleInsertStatement.setMultiTableInsertFragment(mtif);
        }
        treeBuilder.pushStatement(oracleInsertStatement);
        return oracleInsertStatement;
    }

    /**
     * single_table_insert
     * : insert_into_clause (values_clause static_returning_clause? | select_statement) error_logging_clause?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public SingleTableInsertFragment visitSingle_table_insert(PlsqlParser.Single_table_insertContext ctx) {
        SingleTableInsertFragment singleTableInsertFragment = new SingleTableInsertFragment();
        visit(ctx.insert_into_clause());
        InsertIntoClauseFm insertIntoClauseFm = (InsertIntoClauseFm) treeBuilder.popStatement();
        singleTableInsertFragment.setInsertIntoClauseFm(insertIntoClauseFm);
        if (null != ctx.values_clause()) {
            visit(ctx.values_clause());
            ValuesClauseFragment valuesClauseFragment = (ValuesClauseFragment) treeBuilder.popStatement();
            singleTableInsertFragment.setValuesClauseFragment(valuesClauseFragment);
        }
        if (null != ctx.static_returning_clause()) {
            visit(ctx.static_returning_clause());
            StaticReturningClauseFm staticReturningClauseFm = (StaticReturningClauseFm) treeBuilder.popStatement();
            singleTableInsertFragment.setStaticReturningClauseFm(staticReturningClauseFm);
        }
        if (null != ctx.select_statement()) {
            visit(ctx.select_statement());
            OracleSelectStatement oracleSelectStatement = (OracleSelectStatement) treeBuilder.popStatement();
            singleTableInsertFragment.setOracleSelectStatement(oracleSelectStatement);
        }
        if (null != ctx.error_logging_clause()) {
            //TODO ERRORLOG
        }
        treeBuilder.pushStatement(singleTableInsertFragment);
        return singleTableInsertFragment;
    }

    /**
     * insert_into_clause
     * : INTO general_table_ref ('(' column_name (',' column_name)* ')')?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public InsertIntoClauseFm visitInsert_into_clause(PlsqlParser.Insert_into_clauseContext ctx) {
        InsertIntoClauseFm insertIntoClauseFm = new InsertIntoClauseFm();
        visit(ctx.general_table_ref());
        GeneralTableRefFragment generalTableRefFragment = (GeneralTableRefFragment) treeBuilder.popStatement();
        insertIntoClauseFm.setGeneralTableRefFragment(generalTableRefFragment);
        for (PlsqlParser.Column_nameContext columnNameContext : ctx.column_name()) {
            visit(columnNameContext);
            ColumnNameFragment column = (ColumnNameFragment) treeBuilder.popStatement();
            insertIntoClauseFm.addColumnName(column);
        }
        treeBuilder.pushStatement(insertIntoClauseFm);
        return insertIntoClauseFm;
    }

    /**
     * values_clause
     * : VALUES expression_list
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitValues_clause(PlsqlParser.Values_clauseContext ctx) {
        ValuesClauseFragment valuesClauseFragment = new ValuesClauseFragment();
        visit(ctx.expression_list());
        ExpressionListFragment listFragment = (ExpressionListFragment) treeBuilder.popStatement();
        valuesClauseFragment.setExpressionListFragment(listFragment);
        treeBuilder.pushStatement(valuesClauseFragment);
        return valuesClauseFragment;
    }

    /**
     * expression_list
     * : '(' expression? (',' expression)* ')'
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitExpression_list(PlsqlParser.Expression_listContext ctx) {
        ExpressionListFragment expressionListFragment = new ExpressionListFragment();

        for (PlsqlParser.ExpressionContext ec : ctx.expression()) {
            visit(ec);
            ExpressionStatement expressionStatement = (ExpressionStatement) treeBuilder.popStatement();
            expressionListFragment.addExpression(expressionStatement);
        }
        treeBuilder.pushStatement(expressionListFragment);
        return expressionListFragment;
    }

    /**
     * multi_table_insert
     * : (ALL multi_table_element+ | conditional_insert_clause) select_statement
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public MultiTableInsertFragment visitMulti_table_insert(PlsqlParser.Multi_table_insertContext ctx) {
        MultiTableInsertFragment multiTableInsertFragment = new MultiTableInsertFragment();
        for (PlsqlParser.Multi_table_elementContext mtec : ctx.multi_table_element()) {
            visit(mtec);
            MultiTableElementFm mtef = (MultiTableElementFm) treeBuilder.popStatement();
            multiTableInsertFragment.addMutiTableEleFm(mtef);
        }
        if (null != ctx.conditional_insert_clause()) {
            visit(ctx.conditional_insert_clause());
            ConditionalInsertClauseFm conditionalInsertClauseFm = (ConditionalInsertClauseFm) treeBuilder.popStatement();
            multiTableInsertFragment.setClauseFm(conditionalInsertClauseFm);
        }
        visit(ctx.select_statement());
        OracleSelectStatement oracleSelectStatement = (OracleSelectStatement) treeBuilder.popStatement();
        multiTableInsertFragment.setOracleSelectStatement(oracleSelectStatement);
        treeBuilder.pushStatement(multiTableInsertFragment);
        return multiTableInsertFragment;
    }

    /**
     * multi_table_element
     * : insert_into_clause values_clause? error_logging_clause?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public MultiTableElementFm visitMulti_table_element(PlsqlParser.Multi_table_elementContext ctx) {
        MultiTableElementFm multiTableElementFm = new MultiTableElementFm();
        visit(ctx.insert_into_clause());
        InsertIntoClauseFm insertIntoClauseFm = (InsertIntoClauseFm) treeBuilder.popStatement();
        multiTableElementFm.setInsertIntoClauseFm(insertIntoClauseFm);
        if (null != ctx.values_clause()) {
            visit(ctx.values_clause());
            ValuesClauseFragment valuesClauseFragment = (ValuesClauseFragment) treeBuilder.popStatement();
            multiTableElementFm.setValuesClauseFragment(valuesClauseFragment);
        }
        if (null != ctx.error_logging_clause()) {
            //TODO ERRORLOG
        }
        treeBuilder.pushStatement(multiTableElementFm);
        return multiTableElementFm;
    }

    /**
     * conditional_insert_clause
     * : (ALL | FIRST)? conditional_insert_when_part+ conditional_insert_else_part?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitConditional_insert_clause(PlsqlParser.Conditional_insert_clauseContext ctx) {
        ConditionalInsertClauseFm cicfm = new ConditionalInsertClauseFm();
        if (null != ctx.ALL()) {
            cicfm.setAll("ALL");
        }
        if (null != ctx.FIRST()) {
            cicfm.setAll("FIRST");
        }
        for (PlsqlParser.Conditional_insert_when_partContext ciwp : ctx.conditional_insert_when_part()) {
            visit(ciwp);
            ConditionalWhenPartFm conditionalWhenPartFm = (ConditionalWhenPartFm) treeBuilder.popStatement();
            cicfm.addConditionalWhenPart(conditionalWhenPartFm);
        }
        if (null != ctx.conditional_insert_else_part()) {
            visit(ctx.conditional_insert_else_part());
            ConditionalElsePartFm elsePartFm = (ConditionalElsePartFm) treeBuilder.popStatement();
            cicfm.setConditionalElsePartFm(elsePartFm);
        }
        treeBuilder.pushStatement(cicfm);
        return cicfm;
    }

    /**
     * conditional_insert_when_part
     * : WHEN condition THEN multi_table_element+
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public ConditionalWhenPartFm visitConditional_insert_when_part(PlsqlParser.Conditional_insert_when_partContext ctx) {
        ConditionalWhenPartFm conditionalWhenPartFm = new ConditionalWhenPartFm();
        conditionalWhenPartFm.setEs(visitCondition(ctx.condition()));
        for (PlsqlParser.Multi_table_elementContext mtec : ctx.multi_table_element()) {
            visit(mtec);
            MultiTableElementFm multiTableElementFm = (MultiTableElementFm) treeBuilder.popStatement();
            conditionalWhenPartFm.addMultiTableEleFm(multiTableElementFm);
        }
        treeBuilder.pushStatement(conditionalWhenPartFm);
        return conditionalWhenPartFm;
    }

    /**
     * conditional_insert_else_part
     * : ELSE multi_table_element+
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitConditional_insert_else_part(PlsqlParser.Conditional_insert_else_partContext ctx) {
        ConditionalElsePartFm conditionalElsePartFm = new ConditionalElsePartFm();
        for (PlsqlParser.Multi_table_elementContext mtec : ctx.multi_table_element()) {
            visit(mtec);
            MultiTableElementFm multiTableElementFm = (MultiTableElementFm) treeBuilder.popStatement();
            conditionalElsePartFm.addMultiTabelEle(multiTableElementFm);
        }
        treeBuilder.pushStatement(conditionalElsePartFm);
        return conditionalElsePartFm;
    }
    //==================================mergeInto====================================

    /**
     * merge_statement
     * : MERGE INTO tableview_name table_alias? USING selected_tableview ON '(' condition ')'
     * (merge_update_clause merge_insert_clause? | merge_insert_clause merge_update_clause?)?
     * error_logging_clause?
     * <p>
     * examples:
     * merge into crud001 p using crud001 np on (p.name = np.name)
     * when matched then
     * update set p.name = np.name
     * when not matched then
     * insert values(np.name, np.age, np.sex,np.foo)
     *
     * @param ctx
     * @return
     */
    @Override
    public OracleMergeIntoStatment visitMerge_statement(PlsqlParser.Merge_statementContext ctx) {
        OracleMergeIntoStatment oracleMergeIntoStatment = new OracleMergeIntoStatment();

        visit(ctx.tableview_name());
        TableViewNameFragment tableViewNameFragment = (TableViewNameFragment) treeBuilder.popStatement();
        oracleMergeIntoStatment.setTableViewNameFragment(tableViewNameFragment);

        if (null != ctx.table_alias()) {
            visit(ctx.table_alias());
            TableAliasFragment tableAliasFragment = (TableAliasFragment) treeBuilder.popStatement();
            oracleMergeIntoStatment.setTableAliasFragment(tableAliasFragment);
        }
        visit(ctx.selected_tableview());
        SelectTableViewFragment selectTableViewFragment = (SelectTableViewFragment) treeBuilder.popStatement();
        oracleMergeIntoStatment.setSelectTableViewFragment(selectTableViewFragment);

        visit(ctx.condition());
        ExpressionStatement expressionStatement = (ExpressionStatement) treeBuilder.popStatement();
        oracleMergeIntoStatment.setExpressionStatement(expressionStatement);

        if (null != ctx.merge_insert_clause()) {
            visit(ctx.merge_insert_clause());
            MergerInsertClauseFm mergerInsertClauseFm = (MergerInsertClauseFm) treeBuilder.popStatement();
            oracleMergeIntoStatment.setMergerInsertClauseFm(mergerInsertClauseFm);
        }

        if (null != ctx.merge_update_clause()) {
            visit(ctx.merge_update_clause());
            MergerUpdateClauseFm mergerUpdateClauseFm = (MergerUpdateClauseFm) treeBuilder.popStatement();
            oracleMergeIntoStatment.setMergerUpdateClauseFm(mergerUpdateClauseFm);
        }

        if (null != ctx.error_logging_clause()) {
            //TODO
        }
        treeBuilder.pushStatement(oracleMergeIntoStatment);
        return oracleMergeIntoStatment;
    }

    /**
     * merge_update_clause
     * : WHEN MATCHED THEN UPDATE SET merge_element (',' merge_element)* where_clause? merge_update_delete_part?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public MergerUpdateClauseFm visitMerge_update_clause(PlsqlParser.Merge_update_clauseContext ctx) {
        MergerUpdateClauseFm mergeUpdateClauseFm = new MergerUpdateClauseFm();

        for (PlsqlParser.Merge_elementContext me : ctx.merge_element()) {
            visit(me);
            MergerElementFragement mergerElementFragement = (MergerElementFragement) treeBuilder.popStatement();
            mergeUpdateClauseFm.addMergerElem(mergerElementFragement);
        }
        if (null != ctx.where_clause()) {
            visit(ctx.where_clause());
            WhereClauseFragment whereClauseFragment = (WhereClauseFragment) treeBuilder.popStatement();
            mergeUpdateClauseFm.setWhereClauseFragment(whereClauseFragment);
        }
        if (null != ctx.merge_update_delete_part()) {
            visit(ctx.merge_update_delete_part());
            MergerUpdateDelFragment mergerUpdateDelFragment = (MergerUpdateDelFragment) treeBuilder.popStatement();
            mergeUpdateClauseFm.setMergerUpdateDelFragment(mergerUpdateDelFragment);
        }
        treeBuilder.pushStatement(mergeUpdateClauseFm);
        return mergeUpdateClauseFm;
    }

    /**
     * merge_update_delete_part
     * : DELETE where_clause
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitMerge_update_delete_part(PlsqlParser.Merge_update_delete_partContext ctx) {
        MergerUpdateDelFragment mergerUpdateDelFragment = new MergerUpdateDelFragment();
        visit(ctx.where_clause());
        WhereClauseFragment whereClauseFragment = (WhereClauseFragment) treeBuilder.popStatement();
        mergerUpdateDelFragment.setWhereClauseFragment(whereClauseFragment);

        treeBuilder.pushStatement(mergerUpdateDelFragment);
        return mergerUpdateDelFragment;
    }

    /**
     * merge_element
     * : column_name '=' expression
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public MergerElementFragement visitMerge_element(PlsqlParser.Merge_elementContext ctx) {
        MergerElementFragement mergerElementFragement = new MergerElementFragement();

        visit(ctx.column_name());
        ColumnNameFragment columnNameFragment = (ColumnNameFragment) treeBuilder.popStatement();
        mergerElementFragement.setColumnNameFragment(columnNameFragment);
        visit(ctx.expression());
        ExpressionStatement expressionStatement = (ExpressionStatement) treeBuilder.popStatement();
        mergerElementFragement.setExpressionStatement(expressionStatement);

        treeBuilder.pushStatement(mergerElementFragement);
        return mergerElementFragement;
    }

    /**
     * selected_tableview
     * : (tableview_name | '(' select_statement ')') table_alias?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public SelectTableViewFragment visitSelected_tableview(PlsqlParser.Selected_tableviewContext ctx) {
        SelectTableViewFragment selectTableViewFragment = new SelectTableViewFragment();
        if (null != ctx.tableview_name()) {
            visit(ctx.tableview_name());
            TableViewNameFragment tableViewNameFragment = (TableViewNameFragment) treeBuilder.popStatement();
            selectTableViewFragment.setTableViewNameFragment(tableViewNameFragment);
        }
        if (null != ctx.select_statement()) {
            visit(ctx.select_statement());
            OracleSelectStatement oracleSelectStatement = (OracleSelectStatement) treeBuilder.popStatement();
            selectTableViewFragment.setSelectStatement(oracleSelectStatement);
        }
        if (null != ctx.table_alias()) {
            visit(ctx.table_alias());
            TableAliasFragment tableAliasFragment = (TableAliasFragment) treeBuilder.popStatement();
            selectTableViewFragment.setTableAliasFragment(tableAliasFragment);
        }
        treeBuilder.pushStatement(selectTableViewFragment);
        return selectTableViewFragment;
    }

    /**
     * merge_insert_clause
     * : WHEN NOT MATCHED THEN INSERT ('(' column_name (',' column_name)* ')')? VALUES expression_list where_clause?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public MergerInsertClauseFm visitMerge_insert_clause(PlsqlParser.Merge_insert_clauseContext ctx) {
        MergerInsertClauseFm mergerInsertClauseFm = new MergerInsertClauseFm();
        for (PlsqlParser.Column_nameContext column_nameContext : ctx.column_name()) {
            visit(column_nameContext);
            ColumnNameFragment columnNameFragment = (ColumnNameFragment) treeBuilder.popStatement();
            mergerInsertClauseFm.addColumnName(columnNameFragment);
        }
        if (null != ctx.where_clause()) {
            visit(ctx.where_clause());
            WhereClauseFragment whereFrag = (WhereClauseFragment) treeBuilder.popStatement();
            mergerInsertClauseFm.setWhereClauseFragment(whereFrag);
        }
        if (null != ctx.expression_list()) {
            visit(ctx.expression_list());
            ExpressionListFragment es = (ExpressionListFragment) treeBuilder.popStatement();
            mergerInsertClauseFm.setExpressionListFragment(es);
        }
        treeBuilder.pushStatement(mergerInsertClauseFm);
        return mergerInsertClauseFm;
    }

    //=======================================explain================================

    /**
     * explain_statement
     * : EXPLAIN PLAN (SET STATEMENT_ID '=' quoted_string)? (INTO tableview_name)?
     * FOR (select_statement | update_statement | delete_statement | insert_statement | merge_statement)
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public OracleExplainStatment visitExplain_statement(PlsqlParser.Explain_statementContext ctx) {
        OracleExplainStatment explainStatment = new OracleExplainStatment();
        if (null != ctx.quoted_string()) {
            explainStatment.setQuotedStr(getFullSql(ctx.quoted_string()));
        }
        if (null != ctx.tableview_name()) {
            visit(ctx.tableview_name());
            TableViewNameFragment tableViewNameFragment = (TableViewNameFragment) treeBuilder.popStatement();
            explainStatment.setTableViewNameFragment(tableViewNameFragment);
        }
        if (null != ctx.select_statement()) {
            visit(ctx.select_statement());
            OracleSelectStatement oracleSelectStatement = (OracleSelectStatement) treeBuilder.popStatement();
            explainStatment.setOracleSelectStatement(oracleSelectStatement);
        }
        if (null != ctx.delete_statement()) {
            visit(ctx.delete_statement());
            OracleDelStatement delStatement = (OracleDelStatement) treeBuilder.popStatement();
            explainStatment.setOracleDelStatement(delStatement);
        }
        if (null != ctx.update_statement()) {
            visit(ctx.update_statement());
            OracleUpdateStatement updateStatement = (OracleUpdateStatement) treeBuilder.popStatement();
            explainStatment.setOracleUpdateStatement(updateStatement);
        }
        if (null != ctx.insert_statement()) {
            visit(ctx.insert_statement());
            OracleInsertStatement insertStatement = (OracleInsertStatement) treeBuilder.popStatement();
            explainStatment.setOracleInsertStatement(insertStatement);
        }
        treeBuilder.pushStatement(explainStatment);
        return explainStatment;
    }

    /**
     * simple_case_clause
     * : else xx;
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitCase_else_part(PlsqlParser.Case_else_partContext ctx) {
        CaseElsePartStatement elseStatement = new CaseElsePartStatement();
        visit(ctx.seq_of_statements());
        treeBuilder.addNode(elseStatement);
        treeBuilder.pushStatement(elseStatement);
        return elseStatement;
    }

    /**
     * simple_case_clause
     * :
     * ;   when x then xx;
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitSimple_case_when_part(PlsqlParser.Simple_case_when_partContext ctx) {
        CaseWhenPartStatement whenStatement = new CaseWhenPartStatement(true);
        PlsqlParser.ExpressionContext expression = ctx.expression().get(0);
        visit(expression);
        TreeNode condition = treeBuilder.popStatement();
        whenStatement.setCondtion(condition);
        visit(ctx.seq_of_statements());
        treeBuilder.addNode(whenStatement);
        treeBuilder.pushStatement(whenStatement);
        return whenStatement;
    }

    /**
     * simple_case_clause
     * : case u
     * ;   when x then xx;
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitSimple_case_statement(PlsqlParser.Simple_case_statementContext ctx) {
        CaseStatement caseStatement = new CaseStatement(true);
        caseStatement.setSwitchVar(ctx.atom().getText());
        List<PlsqlParser.Simple_case_when_partContext> whens = ctx.simple_case_when_part();
        for (PlsqlParser.Simple_case_when_partContext whenc : whens) {
            visit(whenc);
            treeBuilder.addNode(caseStatement);
        }
        List<TreeNode> whenParts = caseStatement.getChildrenNodes();
        if (whenParts.size() > 0) {
            if (whenParts.get(0) instanceof CaseWhenPartStatement) {
                ((CaseWhenPartStatement) whenParts.get(0)).setFirst();
            }
        }
        for (TreeNode node : whenParts) {
            if (node instanceof CaseWhenPartStatement) {
                ((CaseWhenPartStatement) node).setSwitchVar(caseStatement.getSwitchVar());
            }
        }

        PlsqlParser.Case_else_partContext elsepart = ctx.case_else_part();
        if (elsepart != null) {
            visit(ctx.case_else_part());
            treeBuilder.addNode(caseStatement);
        }
        treeBuilder.pushStatement(caseStatement);
        return caseStatement;
    }

    /**
     * simple_case_clause
     * :
     * ;   when u=x then xx;
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitSearched_case_when_part(PlsqlParser.Searched_case_when_partContext ctx) {
        CaseWhenPartStatement whenStatement = new CaseWhenPartStatement(false);
        PlsqlParser.ExpressionContext expression = ctx.expression().get(0);
        visit(expression);
        TreeNode condition = treeBuilder.popStatement();
        whenStatement.setCondtion(condition);
        visit(ctx.seq_of_statements());
        treeBuilder.addNode(whenStatement);
        treeBuilder.pushStatement(whenStatement);
        return whenStatement;
    }

    /**
     * searched_case_clause
     * : case
     * ;   when u=x then xx;
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitSearched_case_statement(PlsqlParser.Searched_case_statementContext ctx) {
        CaseStatement caseStatement = new CaseStatement(false);
        List<PlsqlParser.Searched_case_when_partContext> whens = ctx.searched_case_when_part();
        for (PlsqlParser.Searched_case_when_partContext whenc : whens) {
            visit(whenc);
            treeBuilder.addNode(caseStatement);
        }
        List<TreeNode> whenParts = caseStatement.getChildrenNodes();
        if (whenParts.size() > 0) {
            if (whenParts.get(0) instanceof CaseWhenPartStatement) {
                ((CaseWhenPartStatement) whenParts.get(0)).setFirst();
            }
        }
        PlsqlParser.Case_else_partContext elsepart = ctx.case_else_part();
        if (elsepart != null) {
            visit(ctx.case_else_part());
            treeBuilder.addNode(caseStatement);
        }
        treeBuilder.pushStatement(caseStatement);
        return caseStatement;
    }

    @Override
    public Object visitRecord_type_dec(PlsqlParser.Record_type_decContext ctx) {
        LocalTypeDeclare typeDeclare = new RecordTypeDeclare();
        typeDeclare.setTypeName(ctx.type_name().getText());
        for (PlsqlParser.Field_specContext fieldCtx : ctx.field_spec()) {
            String fieldVarName = fieldCtx.column_name().getText();
            Var fieldVar = genVarBasedTypeSpec(fieldVarName, fieldCtx.type_spec());
            typeDeclare.addTypeVar(fieldVarName, fieldVar);
        }
        treeBuilder.pushStatement(typeDeclare);
        return typeDeclare;
    }

    @Override
    public Object visitTable_type_dec(PlsqlParser.Table_type_decContext ctx) {
        LocalTypeDeclare typeDeclare = new TableTypeDeclare();
        typeDeclare.setTypeName(ctx.type_name().getText());
        Var arrayTypeVar = genVarBasedTypeSpec("", ctx.type_spec());
        typeDeclare.setArrayVar(arrayTypeVar);
        treeBuilder.pushStatement(typeDeclare);
        return typeDeclare;
    }


    //==============================DDL=====================================


    /**
     * alter_table
     * : ALTER TABLE tableview_name (ADD column_name type_spec column_constraint?
     * | MODIFY column_name type_spec column_constraint?
     * | DROP column_name type_spec column_constraint?) ';'
     *
     * @param ctx
     * @return
     */
    @Override
    public AlterTableFragment visitAlter_table(PlsqlParser.Alter_tableContext ctx) {
        AlterTableFragment alterTableFragment = new AlterTableFragment();

        visit(ctx.tableview_name());
        TableViewNameFragment tableViewNameFragment = (TableViewNameFragment) treeBuilder.popStatement();
        alterTableFragment.setTableViewNameFragment(tableViewNameFragment);

        if (null != ctx.add_column_clause()) {
            visit(ctx.add_column_clause());
            AddColumnFragment addColumnFragment = (AddColumnFragment) treeBuilder.popStatement();
            alterTableFragment.setAddColumnFragment(addColumnFragment);
        }
        if (null != ctx.drop_column_clause()) {
            visit(ctx.drop_column_clause());
            DropColumnFragment dropColumnFragment = (DropColumnFragment) treeBuilder.popStatement();
            alterTableFragment.setDropColumnFragment(dropColumnFragment);
        }
        if (null != ctx.modify_column_clause()) {
            visit(ctx.modify_column_clause());
            ModifyColumnFragment modifyColumnFragment = (ModifyColumnFragment) treeBuilder.popStatement();
            alterTableFragment.setModifyColumnFragment(modifyColumnFragment);
        }
        if (null != ctx.rename_column_clause()) {
            visit(ctx.rename_column_clause());
            ReNameColumnFragment reNameColumnFragment = (ReNameColumnFragment) treeBuilder.popStatement();
            alterTableFragment.setReNameColumnFragment(reNameColumnFragment);
        }
        treeBuilder.pushStatement(alterTableFragment);
        return alterTableFragment;


    }

    /**
     * add_column_clause:
     * ADD column_name type_spec column_constraint?
     *
     * @param ctx
     * @return
     */
    @Override
    public AddColumnFragment visitAdd_column_clause(PlsqlParser.Add_column_clauseContext ctx) {
        AddColumnFragment addColumnFragment = new AddColumnFragment();

        if (null != ctx.column_name()) {
            visit(ctx.column_name());
            ColumnNameFragment columnNameFragment = (ColumnNameFragment) treeBuilder.popStatement();
            addColumnFragment.setColumnNameFragment(columnNameFragment);
        }
        if (null != ctx.type_spec()) {
            Var.DataType type = visitType_spec(ctx.type_spec());
            addColumnFragment.setDataType(type);
        }
        if (null != ctx.column_constraint() && StringUtils.isNotBlank(ctx.column_constraint().getText())) {
            treeBuilder.addException("column default value ", ctx.column_constraint());
        }

        treeBuilder.pushStatement(addColumnFragment);
        return addColumnFragment;
    }


    /**
     * rename_column_clause:
     * RENAME column_name TO column_name
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public ReNameColumnFragment visitRename_column_clause(PlsqlParser.Rename_column_clauseContext ctx) {
        ReNameColumnFragment reNameColumnFragment = new ReNameColumnFragment();

        visit(ctx.column_name(0));
        ColumnNameFragment columnNameFragment = (ColumnNameFragment) treeBuilder.popStatement();
        reNameColumnFragment.setSrcColumnNameFragment(columnNameFragment);

        visit(ctx.column_name(1));
        ColumnNameFragment newColumnNameFragment = (ColumnNameFragment) treeBuilder.popStatement();
        reNameColumnFragment.setNewColumnNameFragment(newColumnNameFragment);

        treeBuilder.pushStatement(reNameColumnFragment);
        return reNameColumnFragment;
    }

    /**
     * drop_column_clause:
     * DROP COLUMN column_name
     *
     * @param ctx
     * @return
     */
    @Override
    public DropColumnFragment visitDrop_column_clause(PlsqlParser.Drop_column_clauseContext ctx) {
        DropColumnFragment dropColumnFragment = new DropColumnFragment();
        visit(ctx.column_name());
        ColumnNameFragment columnNameFragment = (ColumnNameFragment) treeBuilder.popStatement();
        dropColumnFragment.setColumnNameFragment(columnNameFragment);

        treeBuilder.pushStatement(dropColumnFragment);
        return dropColumnFragment;
    }

    /**
     * modify_column_clause:
     * MODIFY column_name type_spec column_constraint?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public ModifyColumnFragment visitModify_column_clause(PlsqlParser.Modify_column_clauseContext ctx) {
        ModifyColumnFragment modifyColumnFragment = new ModifyColumnFragment();
        if (null != ctx.column_name()) {
            visit(ctx.column_name());
            ColumnNameFragment columnNameFragment = (ColumnNameFragment) treeBuilder.popStatement();
            modifyColumnFragment.setColumnNameFragment(columnNameFragment);
        }
        if (null != ctx.type_spec()) {
            Var.DataType type = visitType_spec(ctx.type_spec());
            modifyColumnFragment.setDataType(type);
        }
        if (null != ctx.column_constraint() && StringUtils.isNotBlank(ctx.column_constraint().getText())) {
            treeBuilder.addException("column default value ", ctx.column_constraint());
        }
        treeBuilder.pushStatement(modifyColumnFragment);
        return modifyColumnFragment;
    }


    /**
     * create_package
     * : CREATE (OR REPLACE)? PACKAGE (package_spec | package_body)? ';'
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public CreatePackageStatement visitCreate_package(PlsqlParser.Create_packageContext ctx) {
        CreatePackageStatement createPackageStatement = new CreatePackageStatement();
        if (null != ctx.REPLACE()) {
            createPackageStatement.setReplace(true);
        }
        if (null != ctx.package_spec()) {
            visit(ctx.package_spec());
            PackageSepcFragement packageSepcFragement = (PackageSepcFragement) treeBuilder.popStatement();
            createPackageStatement.setPackageSepcFragement(packageSepcFragement);
        }
        if (null != ctx.package_body()) {
            visit(ctx.package_body());
            PackageBodyFragment packageBodyFragment = (PackageBodyFragment) treeBuilder.popStatement();
            createPackageStatement.setPackageBodyFragment(packageBodyFragment);
        }
        treeBuilder.pushStatement(createPackageStatement);
        return createPackageStatement;
    }

    /**
     * package_spec
     * : package_name invoker_rights_clause? (IS | AS) package_obj_spec* END package_name?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public PackageSepcFragement visitPackage_spec(PlsqlParser.Package_specContext ctx) {
        PackageSepcFragement packageSepcFragement = new PackageSepcFragement();
        if (null != ctx.invoker_rights_clause()) {
            visit(ctx.invoker_rights_clause());
            InvokerRightsAuthFragment invokerRightsAuthFragment = (InvokerRightsAuthFragment) treeBuilder.popStatement();
            packageSepcFragement.setInvokerRightsAuthFragment(invokerRightsAuthFragment);
        }
        visit(ctx.package_name(0));
        PackageNameFragment packageNameFragment = (PackageNameFragment) treeBuilder.popStatement();
        packageSepcFragement.setPackageNameFragment(packageNameFragment);
        for (PlsqlParser.Package_obj_specContext obj : ctx.package_obj_spec()) {
            visit(obj);
            PackageObjSpecFragment packageObjSpecFragment = (PackageObjSpecFragment) treeBuilder.popStatement();
            packageSepcFragement.addPackageObj(packageObjSpecFragment);
        }
        treeBuilder.pushStatement(packageSepcFragement);
        return packageSepcFragement;
    }


    /**
     * package_name
     * : id
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public Object visitPackage_name(PlsqlParser.Package_nameContext ctx) {
        PackageNameFragment packageNameFragment = new PackageNameFragment();
        visit(ctx.id());
        IdFragment idFragment = (IdFragment) treeBuilder.popStatement();
        packageNameFragment.setIdFragment(idFragment);
        treeBuilder.pushStatement(packageNameFragment);
        return packageNameFragment;
    }

    /**
     * invoker_rights_clause
     * : AUTHID (CURRENT_USER|DEFINER)
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public InvokerRightsAuthFragment visitInvoker_rights_clause(PlsqlParser.Invoker_rights_clauseContext ctx) {
        InvokerRightsAuthFragment invokerRightsAuthFragment = new InvokerRightsAuthFragment();
        if (null != ctx.CURRENT_USER()) {
            invokerRightsAuthFragment.setAuthid("CURRENT_USER");
        } else {
            invokerRightsAuthFragment.setAuthid("DEFINER");
        }
        treeBuilder.pushStatement(invokerRightsAuthFragment);
        return invokerRightsAuthFragment;
    }

    /**
     * drop_package
     * : DROP PACKAGE BODY? package_name ';'
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public DropPackageStatement visitDrop_package(PlsqlParser.Drop_packageContext ctx) {
        DropPackageStatement dropPackageStatement = new DropPackageStatement();
        visit(ctx.package_name());
        PackageNameFragment packageNameFragment = (PackageNameFragment) treeBuilder.popStatement();
        dropPackageStatement.setPackageNameFragment(packageNameFragment);
        if (null != ctx.BODY()) {
            dropPackageStatement.setBody(true);
        }
        treeBuilder.pushStatement(dropPackageStatement);
        return dropPackageStatement;
    }

    /**
     * alter_package
     * : ALTER PACKAGE package_name COMPILE DEBUG? (PACKAGE | BODY | SPECIFICATION)? compiler_parameters_clause* (REUSE SETTINGS)? ';'
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public AlterPackageStatement visitAlter_package(PlsqlParser.Alter_packageContext ctx) {
        AlterPackageStatement alterPackageStatement = new AlterPackageStatement();
        //TODO
        treeBuilder.pushStatement(alterPackageStatement);
        return alterPackageStatement;
    }


    /**
     * drop_table
     * : DROP TABLE tableview_name ';'
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public OracleDropTableStatement visitDrop_table(PlsqlParser.Drop_tableContext ctx) {
        OracleDropTableStatement dropTableStatement = new OracleDropTableStatement(Common.DROP_TABLE);
        if (ctx.tableview_name() != null) {
            visit(ctx.tableview_name());
            TableViewNameFragment tableStatement = (TableViewNameFragment) treeBuilder.popStatement();
            dropTableStatement.settableNameStatement(tableStatement);
        }
        treeBuilder.pushStatement(dropTableStatement);
        return dropTableStatement;
    }

    /**
     * drop_view
     * : DROP VIEW tableview_name ';'
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public OracleDropViewStatement visitDrop_view(PlsqlParser.Drop_viewContext ctx) {
        OracleDropViewStatement dropViewStatement = new OracleDropViewStatement(Common.DROP_VIEW);
        if (ctx.tableview_name() != null) {
            visit(ctx.tableview_name());
            TableViewNameFragment viewStatement = (TableViewNameFragment) treeBuilder.popStatement();
            dropViewStatement.setviewNameStatement(viewStatement);
        }
        treeBuilder.pushStatement(dropViewStatement);
        return dropViewStatement;
    }

    /**
     * truncate_table
     * : TRUNCATE TABLE tableview_name ';'
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public OracleTruncateTableStatement visitTruncate_table(PlsqlParser.Truncate_tableContext ctx) {
        OracleTruncateTableStatement truncateTableStatement = new OracleTruncateTableStatement();
        if (ctx.tableview_name() != null) {
            visit(ctx.tableview_name());
            TableViewNameFragment tTableStatement = (TableViewNameFragment) treeBuilder.popStatement();
            truncateTableStatement.setTableViewNameStatement(tTableStatement);
        }
        treeBuilder.pushStatement(truncateTableStatement);
        return truncateTableStatement;
    }


    /**
     * create_view
     * : CREATE (OR REPLACE)? VIEW tableview_name ('('column_name?  (',' column_name )*  ')' )? AS subquery
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public OracleCreateViewStatment visitCreate_view(PlsqlParser.Create_viewContext ctx) {
        OracleCreateViewStatment oracleCreateViewStatment = new OracleCreateViewStatment();


        if (null != ctx.REPLACE()) {
            oracleCreateViewStatment.setReplace(true);
        }

        visit(ctx.tableview_name());
        TableViewNameFragment tableViewNameFragment = (TableViewNameFragment) treeBuilder.popStatement();
        oracleCreateViewStatment.setTableViewNameFragment(tableViewNameFragment);

        for (PlsqlParser.Column_nameContext column_nameContext : ctx.column_name()) {
            visit(column_nameContext);
            ColumnNameFragment columnNameFragment = (ColumnNameFragment) treeBuilder.popStatement();
            oracleCreateViewStatment.addColumnName(columnNameFragment);
        }

        visit(ctx.subquery());
        SubqueryFragment subqueryFragment = (SubqueryFragment) treeBuilder.popStatement();
        oracleCreateViewStatment.setSubqueryFragment(subqueryFragment);

        treeBuilder.pushStatement(oracleCreateViewStatment);
        return oracleCreateViewStatment;
    }


    @Override
    public SqlStatement visitShow_tables(PlsqlParser.Show_tablesContext ctx) {
        SqlStatement sqlStatement = new SqlStatement(Common.SHOW_TABLES);
        StringBuffer sql = new StringBuffer();
        sql.append(ctx.SHOW().getText()).append(Common.SPACE);
        sql.append(ctx.TABLES().getText()).append(Common.SPACE);
        sqlStatement.setSql(sql.toString());
        sqlStatement.setAddResult(true);
        treeBuilder.pushStatement(sqlStatement);
        return sqlStatement;
    }

    @Override
    public SqlStatement visitShow_databases(PlsqlParser.Show_databasesContext ctx) {
        SqlStatement sqlStatement = new SqlStatement(Common.SHOW_TABLES);
        StringBuffer sql = new StringBuffer();
        sql.append(ctx.SHOW().getText()).append(Common.SPACE);
        sql.append(ctx.DATABASES().getText()).append(Common.SPACE);
        sqlStatement.setSql(sql.toString());
        sqlStatement.setAddResult(true);
        treeBuilder.pushStatement(sqlStatement);
        return sqlStatement;
    }

    @Override
    public OracleUseStatement visitUse_statement(PlsqlParser.Use_statementContext ctx) {
        OracleUseStatement oracleUseStatement = new OracleUseStatement();
        visitId(ctx.id());
        IdFragment dbName = (IdFragment) treeBuilder.popStatement();
        oracleUseStatement.setDbName(dbName);
        this.treeBuilder.pushStatement(oracleUseStatement);
        return oracleUseStatement;
    }


}
