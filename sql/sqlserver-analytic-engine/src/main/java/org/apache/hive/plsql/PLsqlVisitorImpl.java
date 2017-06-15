package org.apache.hive.plsql;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.apache.hive.basesql.TreeBuilder;
import org.apache.hive.plsql.block.AnonymousBlock;
import org.apache.hive.plsql.block.ExceptionHandler;
import org.apache.hive.plsql.cfl.OracleReturnStatement;
import org.apache.hive.plsql.dml.OracleSelectStatement;
import org.apache.hive.plsql.dml.fragment.*;
import org.apache.hive.plsql.function.FakeFunction;
import org.apache.hive.plsql.function.Function;
import org.apache.hive.plsql.function.ProcedureCall;
import org.apache.hive.tsql.another.DeclareStatement;
import org.apache.hive.tsql.another.SetStatement;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.cfl.*;
import org.apache.hive.tsql.common.*;
import org.apache.hive.tsql.ddl.CreateProcedureStatement;
import org.apache.hive.tsql.ddl.CreateTableStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;
import org.apache.hive.tsql.func.FuncName;
import org.apache.hive.tsql.func.Procedure;
import org.apache.hive.tsql.node.LogicNode;
import org.apache.hive.tsql.node.PredicateNode;
import org.apache.spark.sql.catalyst.expressions.Exp;
import org.apache.spark.sql.catalyst.plans.logical.Except;

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

    /*@Override
    public Object visitData_manipulation_language_statements(PlsqlParser.Data_manipulation_language_statementsContext ctx) {
        // TODO test only
        String sql = ctx.start.getInputStream().getText(
                new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
        return null;
    }*/

    @Override
    public Object visitCompilation_unit(PlsqlParser.Compilation_unitContext ctx) {
        for (PlsqlParser.Unit_statementContext unitCtx: ctx.unit_statement()) {
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

    /*@Override
    public Object visitDeclare_spec(PlsqlParser.Declare_specContext ctx) {
        return visitChildren(ctx);
    }*/

    @Override
    public Object visitVariable_declaration(PlsqlParser.Variable_declarationContext ctx) {
        DeclareStatement declareStatement = new DeclareStatement();
        Var var = new Var();
        String varName = ctx.variable_name().getText();
        var.setVarName(varName);
        Var.DataType varType = (Var.DataType) visit(ctx.type_spec());
        var.setDataType(varType);
        if (ctx.default_value_part() != null) {
            visit(ctx.default_value_part());
            var.setExpr(treeBuilder.popStatement());
            var.setValueType(Var.ValueType.EXPRESSION);
        }
        declareStatement.addDeclareVar(var);
        treeBuilder.pushStatement(declareStatement);
        return declareStatement;
    }

    @Override
    public Object visitType_spec(PlsqlParser.Type_specContext ctx) {
        String typeName = "";
        if (ctx.datatype() != null) {
            // receive from native_datatype_element
            typeName = (String) visit(ctx.datatype());
        }
        switch (typeName.toUpperCase()) {
            case "BINARY_INTEGER":
            case "PLS_INTEGER":
            case "INTEGER":
            case "INT":
            case "NUMERIC":
            case "SMALLINT":
            case "NUMBER":
            case "DECIMAL":
            case "FLOAT":
                return Var.DataType.INT;
            case "VARCHAR2":
            case "VARCHAR":
            case "STRING":
                return Var.DataType.STRING;
            case "BOOLEAN":
                // TODO need a top expression include logicNode and expressionStatement
                return Var.DataType.BOOLEAN;
            default:
                return Var.DataType.DEFAULT;
        }
    }

    @Override
    public Object visitNative_datatype_element(PlsqlParser.Native_datatype_elementContext ctx) {
        return ctx.getText();
    }

    /*@Override
    public Object visitDefault_value_part(PlsqlParser.Default_value_partContext ctx) {
        visit(ctx.expression());
        return null;
    }*/

    /*@Override
    public Object visitExpression(PlsqlParser.ExpressionContext ctx) {
        // bool expression
        if (ctx.OR() != null || ctx.logical_and_expression() != null) {
        }
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

    /*private ExpressionStatement genBinary_expression(List<PlsqlParser.ExpressionContext> exprs, String op) {
        ExpressionBean expressionBean = new ExpressionBean();
        ExpressionStatement es = new ExpressionStatement(expressionBean);
        expressionBean.setOperatorSign(OperatorSign.getOpator(op));
        if (exprs.size() != 2) {
            // TODO exception
        }
        for (PlsqlParser.ExpressionContext expr: exprs) {
            visit(expr);
            treeBuilder.addNode(es);
        }
        return es;
    }*/

    @Override
    public Object visitBinary_expression_alias(PlsqlParser.Binary_expression_aliasContext ctx) {
        ExpressionBean expressionBean = new ExpressionBean();
        ExpressionStatement es = new ExpressionStatement(expressionBean);
        expressionBean.setOperatorSign(OperatorSign.getOpator(ctx.op.getText()));
        List<PlsqlParser.ExpressionContext> exprList = ctx.expression();
        if (exprList.size() != 2) {
            // TODO exception
        }
        for (PlsqlParser.ExpressionContext expr: exprList) {
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
        for (PlsqlParser.Sub_expressionContext expr: exprList) {
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
        visit(ctx.equality_expression());
        treeBuilder.addNode(orNode);
        treeBuilder.pushStatement(orNode);
        return orNode;
    }

    @Override
    public Object visitRelational_expression(PlsqlParser.Relational_expressionContext ctx) {
        PredicateNode predicateNode = new PredicateNode(TreeNode.Type.PREDICATE);
        predicateNode.setEvalType(PredicateNode.CompType.COMP);
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
            LogicNode conditionNode = (LogicNode) treeBuilder.popStatement();
            ifStatement.setCondtion(conditionNode);
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
        visit(ctx.id());
        TreeNode experssionStmt = treeBuilder.popStatement();
        treeBuilder.pushStatement(experssionStmt);
        return experssionStmt;
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

        for (PlsqlParser.ParameterContext para: ctx.parameter()) {
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
        String functionName = ctx.function_name().getText();
        FuncName funcId = new FuncName();
        funcId.setFuncName(functionName);
        Function function = new Function(funcId);
        function.setProcSql(sourceFunction);
        function.setMd5(MD5Util.md5Hex(sourceFunction));

        for (PlsqlParser.ParameterContext para: ctx.parameter()) {
            function.addInAndOutputs((Var) visit(para));
        }

        visit(ctx.anonymous_block());
        function.setSqlClauses(treeBuilder.popStatement());
        function.setLeastArguments();

        CreateProcedureStatement.Action action = null != ctx.CREATE() ?
                CreateProcedureStatement.Action.CREATE : CreateProcedureStatement.Action.ALTER;
        CreateProcedureStatement statement = new CreateProcedureStatement(function, action);
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
            for (PlsqlParser.Execute_argumentContext argCtx: ctx.execute_argument()) {
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
            for (PlsqlParser.Execute_argumentContext argCtx: ctx.execute_argument()) {
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
        for (PlsqlParser.ArgumentContext argCtx: ctx.argument()) {
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

    @Override
    public Object visitCreate_table(PlsqlParser.Create_tableContext ctx) {
        String tableName = ctx.tableview_name().getText();
        CreateTableStatement createTableStatement = new CreateTableStatement(tableName);
        String colString = genTableColString(ctx.column_name(), ctx.type_spec());
        createTableStatement.setColumnDefs(colString);
        treeBuilder.pushStatement(createTableStatement);
        return createTableStatement;
    }

    @Override
    public Object visitSelect_statement(PlsqlParser.Select_statementContext ctx) {
        OracleSelectStatement selectStatement = new OracleSelectStatement(Common.SELECT);
        if (ctx.subquery_factoring_clause() != null) {
            visit(ctx.subquery_factoring_clause());
            SqlStatement withStatement = (SqlStatement) treeBuilder.popStatement();
            selectStatement.setWithQueryStatement(withStatement);
        }
        visit(ctx.subquery());
        SqlStatement subqueryStmt = (SqlStatement) treeBuilder.popStatement();
        selectStatement.setQueryBlockStatement(subqueryStmt);
        if (ctx.for_update_clause() != null) {
            visit(ctx.for_update_clause());
            SqlStatement forStmt = (SqlStatement) treeBuilder.popStatement();
            selectStatement.setForClauseStatement(forStmt);
        }
        if (ctx.order_by_clause() != null) {
            visit(ctx.order_by_clause());
            SqlStatement orderStmt = (SqlStatement) treeBuilder.popStatement();
            selectStatement.setOrderByStatement(orderStmt);
        }
        treeBuilder.pushStatement(selectStatement);
        return selectStatement;
    }

    @Override
    public Object visitSubquery(PlsqlParser.SubqueryContext ctx) {
        // TODO
        SubqueryFragment subqueryFragment = new SubqueryFragment();
        visit(ctx.subquery_basic_elements());
        SqlStatement basicStmt = (SqlStatement) treeBuilder.popStatement();
        subqueryFragment.setBasicElement(basicStmt);
        for (PlsqlParser.Subquery_operation_partContext opCtx: ctx.subquery_operation_part()) {
            visit(opCtx);
            SqlStatement opStmt = (SqlStatement) treeBuilder.popStatement();
            subqueryFragment.addOperation(opStmt);
        }
        treeBuilder.pushStatement(subqueryFragment);
        return subqueryFragment;
    }

    @Override
    public Object visitQuery_block(PlsqlParser.Query_blockContext ctx) {
        QueryBlockFragment queryBlockFragment = new QueryBlockFragment();
        for (PlsqlParser.Selected_elementContext eleCtx: ctx.selected_element()) {
            visit(eleCtx);
            SqlStatement element = (SqlStatement) treeBuilder.popStatement();
            queryBlockFragment.addElement(element);
        }
        visit(ctx.from_clause());
        SqlStatement fromFrag = (SqlStatement) treeBuilder.popStatement();
        queryBlockFragment.setFromClause(fromFrag);
        if (ctx.where_clause() != null) {
            visit(ctx.where_clause());
            SqlStatement whereFrag = (SqlStatement) treeBuilder.popStatement();
            queryBlockFragment.setWhereClause(whereFrag);
        }
        treeBuilder.pushStatement(queryBlockFragment);
        return queryBlockFragment;
    }

    @Override
    public Object visitSelected_element(PlsqlParser.Selected_elementContext ctx) {
        SelectElementFragment elementFragment = new SelectElementFragment();
        ExpressionStatement col = (ExpressionStatement) visit(ctx.select_list_elements());
        elementFragment.setCol(col);
        if (ctx.column_alias() != null) {
            String colAlias = (String) visit(ctx.column_alias());
            elementFragment.setColAlias(colAlias);
        }
        treeBuilder.pushStatement(elementFragment);
        return elementFragment;
    }

    @Override
    public Object visitSelect_list_elements(PlsqlParser.Select_list_elementsContext ctx) {
        if (ctx.expression() != null) {
            visit(ctx.expression());
            ExpressionStatement es = (ExpressionStatement) treeBuilder.popStatement();
            return es;
        }
        if (ctx.tableview_name() != null) {
            String colStr = getFullSql(ctx.tableview_name());
            Var var = new Var(null, colStr, Var.DataType.STRING);
            ExpressionBean expressionBean = new ExpressionBean();
            expressionBean.setVar(var);
            ExpressionStatement expressionStatement = new ExpressionStatement(expressionBean);
            return expressionStatement;
        }
        return null;
    }

    @Override
    public Object visitColumn_alias(PlsqlParser.Column_aliasContext ctx) {
        return getFullSql(ctx);
    }

    @Override
    public Object visitFrom_clause(PlsqlParser.From_clauseContext ctx) {
        FromClauseFragment fromClauseFragment = new FromClauseFragment();
        visit(ctx.table_ref_list());
        SqlStatement sourceStmt = (SqlStatement) treeBuilder.popStatement();
        fromClauseFragment.setSourceFrag(sourceStmt);
        treeBuilder.pushStatement(fromClauseFragment);
        return fromClauseFragment;
    }

    @Override
    public Object visitTable_ref_list(PlsqlParser.Table_ref_listContext ctx) {
        CommonFragment commonFragment = new CommonFragment();
        for (PlsqlParser.Table_refContext refCtx: ctx.table_ref()) {
            visit(refCtx);
            SqlStatement stmt = (SqlStatement) treeBuilder.popStatement();
            commonFragment.addFragment(stmt);
        }
        treeBuilder.pushStatement(commonFragment);
        return commonFragment;
    }

    @Override
    public Object visitTable_ref(PlsqlParser.Table_refContext ctx) {
        CommonFragment commonFragment = new CommonFragment();
        visit(ctx.table_ref_aux());
        SqlStatement refStmt = (SqlStatement) treeBuilder.popStatement();
        commonFragment.addFragment(refStmt);
        for (PlsqlParser.Join_clauseContext joinCtx: ctx.join_clause()) {
            visit(joinCtx);
        }
        treeBuilder.pushStatement(commonFragment);
        return commonFragment;
    }

    @Override
    public Object visitTable_ref_aux(PlsqlParser.Table_ref_auxContext ctx) {
        CommonFragment commonFragment = new CommonFragment();
        if (ctx.dml_table_expression_clause() != null) {
            if (ctx.ONLY() == null) {
                visit(ctx.dml_table_expression_clause());
                SqlStatement stmt = (SqlStatement) treeBuilder.popStatement();
                commonFragment.addFragment(stmt);
            } else {
            }
        } else if (ctx.table_ref() != null) {
        }
        treeBuilder.pushStatement(commonFragment);
        return commonFragment;
    }

    @Override
    public Object visitDml_table_expression_clause(PlsqlParser.Dml_table_expression_clauseContext ctx) {
        SqlStatement sqlStatement = null;
        if (ctx.tableview_name() != null) {
            sqlStatement = genSqlStringFragment(getFullSql(ctx.tableview_name()));
        } else if (ctx.select_statement() != null) {
            visit(ctx.select_statement());
            CommonFragment commonFragment = new CommonFragment();
            commonFragment.addFragment(genSqlStringFragment("("));
            SqlStatement stmt = (SqlStatement) treeBuilder.popStatement();
            commonFragment.addFragment(stmt);
            commonFragment.addFragment(genSqlStringFragment(")"));
            sqlStatement = commonFragment;
        } else {
            // table_collection_expression != null
        }
        treeBuilder.pushStatement(sqlStatement);
        return sqlStatement;
    }

    @Override
    public Object visitWhere_clause(PlsqlParser.Where_clauseContext ctx) {
        WhereClauseFragment whereClauseFragment = new WhereClauseFragment();
        if (ctx.expression() != null) {
            visit(ctx.expression());
            SqlStatement exprStmt = (SqlStatement) treeBuilder.popStatement();
            whereClauseFragment.setCondition(exprStmt);
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
}
