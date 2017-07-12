package org.apache.hive.plsql;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.hive.basesql.TreeBuilder;
import org.apache.hive.plsql.block.AnonymousBlock;
import org.apache.hive.plsql.block.ExceptionHandler;
import org.apache.hive.plsql.cfl.OracleReturnStatement;
import org.apache.hive.plsql.dml.OracleSelectStatement;
import org.apache.hive.plsql.dml.commonFragment.*;
import org.apache.hive.plsql.dml.fragment.delFragment.OracleDelStatement;
import org.apache.hive.plsql.dml.fragment.explainFragment.OracleExplainStatment;
import org.apache.hive.plsql.dml.fragment.insertFragment.*;
import org.apache.hive.plsql.dml.fragment.selectFragment.*;
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
import org.apache.hive.plsql.function.FakeFunction;
import org.apache.hive.plsql.function.Function;
import org.apache.hive.plsql.function.ProcedureCall;
import org.apache.hive.tsql.another.DeclareStatement;
import org.apache.hive.tsql.another.SetStatement;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.cfl.*;
import org.apache.hive.tsql.common.*;
import org.apache.hive.tsql.ddl.CreateFunctionStatement;
import org.apache.hive.tsql.ddl.CreateProcedureStatement;
import org.apache.hive.tsql.ddl.CreateTableStatement;
import org.apache.hive.tsql.dml.ExpressionStatement;
import org.apache.hive.tsql.func.FuncName;
import org.apache.hive.tsql.func.Procedure;
import org.apache.hive.tsql.node.LogicNode;
import org.apache.hive.tsql.node.PredicateNode;
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
        for (PlsqlParser.Label_declarationContext labelCtx : ctx.label_declaration()) {
            visit(labelCtx);
            treeBuilder.addNode(beginEndStatement);
        }
        for (PlsqlParser.StatementContext stateCtx : ctx.statement()) {
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
        for (PlsqlParser.Declare_specContext declareCtx : ctx.declare_spec()) {
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
        if(expressCtxList.size() != 2){
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
            LogicNode conditionNode = (LogicNode) treeBuilder.popStatement();
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
        String functionName = ctx.function_name().getText();
        FuncName funcId = new FuncName();
        funcId.setFuncName(functionName);
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
    @Override
    public Object visitCreate_table(PlsqlParser.Create_tableContext ctx) {
        String tableName = ctx.tableview_name().getText();
        CreateTableStatement createTableStatement = new CreateTableStatement(tableName);
        String colString = genTableColString(ctx.column_name(), ctx.type_spec());
        createTableStatement.setColumnDefs(colString);
        treeBuilder.pushStatement(createTableStatement);
        return createTableStatement;
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
        List<String> idExpressions = new ArrayList<>();
        columnNameFragment.setId(id);
        for (PlsqlParser.Id_expressionContext idExpressionContext : ctx.id_expression()) {
            String idExpression = visitId_expression(idExpressionContext);
            idExpressions.add(idExpression);
        }
        columnNameFragment.setIdExpressions(idExpressions);
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
    public String visitId_expression(PlsqlParser.Id_expressionContext ctx) {
        return getFullSql(ctx);
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
        List<String> idExpressions = new ArrayList<>();
        for (PlsqlParser.Id_expressionContext idExpressionContext : ctx.id_expression()) {
            String idExpression = visitId_expression(idExpressionContext);
            idExpressions.add(idExpression);
        }
        charSetNameFragment.setIdExpressions(idExpressions);
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
        String idExpression = visitId_expression(ctx.id_expression());
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
        List<OrderByElementsFragment> orderByEles = new ArrayList<>();
        for (PlsqlParser.Order_by_elementsContext order_by_elementsContext : ctx.order_by_elements()) {
            visit(order_by_elementsContext);
            OrderByElementsFragment orderByElement = (OrderByElementsFragment) treeBuilder.popStatement();
            orderByEles.add(orderByElement);
        }
        orderByClause.setOrderByElements(orderByEles);
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
        visit(ctx.from_clause());
        FromClauseFragment fromFrag = (FromClauseFragment) treeBuilder.popStatement();
        queryBlockFragment.setFromClause(fromFrag);
        if (null != ctx.where_clause()) {
            visit(ctx.where_clause());
            WhereClauseFragment whereFrag = (WhereClauseFragment) treeBuilder.popStatement();
            queryBlockFragment.setWhereClause(whereFrag);
        }
        treeBuilder.pushStatement(queryBlockFragment);
        return queryBlockFragment;
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
        ExpressionStatement col = (ExpressionStatement) visit(ctx.select_list_elements());
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
            String idExpression = visitId_expression(ctx.id_expression());
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
            String idExpression = visitId_expression(ide);
            idExpressions.add(idExpression);
        }
        afnf.setIdExpressions(idExpressions);
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
        ExpressionStatement expressionStatement = (ExpressionStatement)treeBuilder.popStatement();
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
        treeBuilder.pushStatement(updateSetClauseFm);
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
            String iecStr = visitId_expression(iec);
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
        visit(ctx.id());
        IdFragment idFragment = (IdFragment) treeBuilder.popStatement();
        updateSetClauseFm.setIdFragment(idFragment);
        visit(ctx.expression());
        ExpressionStatement expressionStatement = (ExpressionStatement) treeBuilder.popStatement();
        updateSetClauseFm.setExpressionStatement(expressionStatement);
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
    public Object visitMerge_statement(PlsqlParser.Merge_statementContext ctx) {
        //TODO MERGEiNTO
        return super.visitMerge_statement(ctx);
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
        CaseElseStatement elseStatement = new CaseElseStatement();
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
        CaseWhenStatement whenStatement = new CaseWhenStatement(true);
        PlsqlParser.ExpressionContext expression = ctx.expression().get(0);
        visit(expression);
        TreeNode condition = treeBuilder.popStatement();
        whenStatement.setCondtion(condition);
        visit(ctx.seq_of_statements());
        treeBuilder.addNode(whenStatement);
        treeBuilder.pushStatement(whenStatement);
        return  whenStatement;
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
        for(PlsqlParser.Simple_case_when_partContext whenc : whens){
            visit(whenc);
            treeBuilder.addNode(caseStatement);
        }
        PlsqlParser.Case_else_partContext elsepart = ctx.case_else_part();
        if(elsepart != null){
            visit(ctx.case_else_part());
            treeBuilder.addNode(caseStatement);
        }
        treeBuilder.pushStatement(caseStatement);
        return caseStatement;
    }

}