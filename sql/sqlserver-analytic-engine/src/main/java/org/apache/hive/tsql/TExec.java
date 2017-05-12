package org.apache.hive.tsql;


import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang.StringUtils;
import org.apache.hive.tsql.another.*;
import org.apache.hive.tsql.arg.Var;
import org.apache.hive.tsql.cfl.*;
import org.apache.hive.tsql.common.*;
import org.apache.hive.tsql.cursor.*;
import org.apache.hive.tsql.ddl.*;
import org.apache.hive.tsql.dml.*;
import org.apache.hive.tsql.dml.mergeinto.*;
import org.apache.hive.tsql.dml.select.QueryExpressionBean;
import org.apache.hive.tsql.dml.select.SelectIntoBean;
import org.apache.hive.tsql.exception.AlreadyDeclaredException;
import org.apache.hive.tsql.exception.Position;
import org.apache.hive.tsql.exception.UnsupportedException;
import org.apache.hive.tsql.func.*;
import org.apache.hive.tsql.node.LogicNode;
import org.apache.hive.tsql.node.PredicateNode;
import org.apache.hive.tsql.sqlsverExcept.ColumnBean;
import org.apache.hive.tsql.sqlsverExcept.QuerySpecificationBean;
import org.apache.hive.tsql.sqlsverExcept.UnionBean;
import org.apache.hive.tsql.util.StrUtils;
import scala.collection.mutable.StringBuilder;

import java.text.ParseException;
import java.util.*;

/**
 * Created by zhongdg1 on 2016/11/23.
 */
public class TExec extends TSqlBaseVisitor<Object> {


    /**
     * for acid table
     */
    private boolean procFlag = false;


    private LinkedList<TreeNode> curNodeStack = new LinkedList<TreeNode>();

    private TreeNode rootNode; //解析编译过程的目标就是构造这个treenode

    private List<Exception> exceptions;

    public TExec(TreeNode rootNode) {
//        this.rootNode = ExecSession.getSession().getRootNode();
        this.rootNode = rootNode;
        exceptions = new ArrayList<>();
    }

    private void addException(String msg, Position position) {
        exceptions.add(new UnsupportedException(msg, position));
    }

    private void addException(Exception exception) {
        exceptions.add(exception);
    }

    public List<Exception> getExceptions() {
        return exceptions;
    }

    @Override
    public Object visitTsql_file(TSqlParser.Tsql_fileContext ctx) {
        return super.visitTsql_file(ctx);
    }

    @Override
    public Integer visitBatch(TSqlParser.BatchContext ctx) {
        //将sql_clauses加入到ROOT TreeNode
        if (ctx.getChildCount() < 1) {
            System.out.println("No sql_clauses found.");
        }

        // support GO repeat execute
        GoStatement goStatement = new GoStatement();
        if (ctx.go_statement() != null) {
            TSqlParser.Go_statementContext goCtx = ctx.go_statement();
            if (goCtx.count != null) {
                try {
                    int count = Integer.parseInt(goCtx.count.getText());
                    if (count <= 0)
                        addException("GO repeat number is illegal", locate(ctx));
                    goStatement.setRepeat(count);
                } catch (Exception e) {
                    addException("GO repeat number is illegal", locate(ctx));
                }
            }
        }
        rootNode.setCurrentGoStmt(goStatement);
        visitSql_clauses(ctx.sql_clauses());
        addNode(goStatement);
        this.rootNode.addNode(goStatement);
        declares.clear(); //only a var in one go statement
        return 0;
    }


    @Override
    public Object visitGo_statement(TSqlParser.Go_statementContext ctx) {
        return super.visitGo_statement(ctx);
    }

    @Override
    public BaseStatement visitSql_clauses(TSqlParser.Sql_clausesContext ctx) {
        SqlClausesStatement sqlClausesStatement = new SqlClausesStatement();
        for (TSqlParser.Sql_clauseContext clause : ctx.sql_clause()) {
            visit(clause);
            addNode(sqlClausesStatement);
        }
        this.pushStatement(sqlClausesStatement);
        return sqlClausesStatement;
    }

    /**
     * USE database=id ';'?
     */
    @Override
    public BaseStatement visitUse_statement(TSqlParser.Use_statementContext ctx) {
        String dbName = visitId(ctx.id());
        StringBuffer sb = new StringBuffer();
        sb.append(ctx.USE().getText()).append(" ").append(dbName);
        UseStatement statement = new UseStatement(sb.toString());

        if (StringUtils.isNotBlank(dbName)) {
            statement.setDbName(dbName.trim());
        }
        this.pushStatement(statement);
        return statement;
    }

    @Override
    public BaseStatement visitExecute_func_proc(TSqlParser.Execute_func_procContext ctx) {
        boolean isWithName = false;
        ExecuteStatement statement = new ExecuteStatement(visitFunc_proc_name(ctx.func_proc_name()));
        for (TSqlParser.Execute_statement_argContext arg : ctx.execute_statement_arg()) {
            Var var = visitExecute_statement_arg(arg);
            statement.addArgument(var);
            if (!isWithName && var.getVarName() != null) {
                isWithName = true;
            }

            if (isWithName && var.getVarName() == null) {
                addException("NonKV arguments follows KV arguments.", locate(ctx));
            }
//            if (null != var.getVarName()) {
//                if (var.getVarType() == Var.VarType.OUTPUT) {
//                    addException(var.getVarName() + " cannot output", locate(ctx));
//                } else {
//                    statement.setWithName(true);
//                }
//            }
        }
        if (null != ctx.return_status) {
            statement.setReturnVarName(ctx.return_status.getText());
        }
        this.addNodeAndPush(statement);
        return statement;
    }


    @Override
    public TreeNode visitExecute_var(TSqlParser.Execute_varContext ctx) {
        ExecuteStringStatement statement = new ExecuteStringStatement();
        StringBuffer sb = new StringBuffer();
        Set<String> vars = new HashSet<>();
        for (TSqlParser.Execute_var_stringContext varStringCtx : ctx.execute_var_string()) {
            VarString varString = visitExecute_var_string(varStringCtx);
            sb.append(StrUtils.trimQuot(varString.getValue())).append(" ");
            if (varString.isVariable()) {
                vars.add(varString.getValue().toUpperCase());
            }
        }
        statement.addLocalVars(vars);
        statement.setSql(sb.toString());
        pushStatement(statement);
        return statement;
    }

    @Override
    public VarString visitExecute_var_string(TSqlParser.Execute_var_stringContext ctx) {
        VarString varString = new VarString(ctx.getText());
        if (null != ctx.LOCAL_ID()) {
            varString.setVariable(true);
        }
        return varString;
    }

    @Override
    public Var visitExecute_statement_arg(TSqlParser.Execute_statement_argContext ctx) {
        String varName = null == ctx.LOCAL_ID() ? null : ctx.LOCAL_ID().getText().toUpperCase();


        Var var = new Var(varName, null, Var.DataType.COMMON);
        if (null != ctx.constant_LOCAL_ID()) {
            ConstantLocalID cons = visitConstant_LOCAL_ID(ctx.constant_LOCAL_ID());
            var.setVarValue(cons.getVal());
            var.setValueType(cons.isVariable() ? Var.ValueType.EXPRESSION : Var.ValueType.NONE);
        }
        if (null != ctx.id()) {
            var.setVarValue(visitId(ctx.id()));
        }
        if (null != ctx.NULL()) {
            var.setVarValue(null);
        }

        if (null != ctx.DEFAULT()) {
            var.setVarValue(null);
            var.setValueType(Var.ValueType.DEFAULT);
        }

        if (ctx.OUT() != null || ctx.OUTPUT() != null) {
            var.setVarType(Var.VarType.OUTPUT);
//            try {
//                var.setVarName(var.getVarValue().toString());
//            } catch (ParseException e) {
//                e.printStackTrace();
//            }
//            var.setVarValue(null);
        }
        return var;
    }


    @Override
    public ConstantLocalID visitConstant_LOCAL_ID(TSqlParser.Constant_LOCAL_IDContext ctx) {
//        return ctx == null ? null : ctx.getText();
        if (null == ctx) {
            return null;
        }
        ConstantLocalID constantLocalID = new ConstantLocalID();
        String str = ctx.getText();
        if (str.startsWith("N")) {
            str = str.substring(1);
        }
        constantLocalID.setVal(str);
        constantLocalID.setVariable(ctx.LOCAL_ID() != null);
        return constantLocalID;
    }

    @Override
    public BaseStatement visitCreate_procedure(TSqlParser.Create_procedureContext ctx) {
        procFlag = true;
        String sourceSql = ctx.start.getInputStream().getText(
                new Interval(ctx.start.getStartIndex(), ctx.stop.getStopIndex()));
        FuncName funcName = visitFunc_proc_name(ctx.func_proc_name());
        checkLength(funcName.getRealFullFuncName(), 128, "procedure name ");
        Procedure func = new Procedure(funcName);
        func.setProcSql(sourceSql);
        func.setMd5(MD5Util.md5Hex(sourceSql));
        for (TSqlParser.Procedure_paramContext pc : ctx.procedure_param()) {
            func.addInAndOutputs(visitProcedure_param(pc));
//            switch (var.getVarType()) {
//                case INPUT:
//                    func.getInputs().add(var);
//                    break;
//                case OUTPUT:
//                    func.getOutputs().add(var);
//                    break;
//            }
        }

//        int inputSize = 0;
//        for (int i = 0; i < func.getInputs().size(); i++) {
//            if (!func.getInputs().get(i).isDefault()) {
//                inputSize = i + 1;
//            }
//        }
//        func.setLeastArguments(inputSize);
        visitSql_clauses(ctx.sql_clauses());
        func.setSqlClauses(this.popStatement());
        CreateProcedureStatement.Action action = null != ctx.CREATE() ? CreateProcedureStatement.Action.CREATE : CreateProcedureStatement.Action.ALTER;
        CreateProcedureStatement statement = new CreateProcedureStatement(func, action);
        pushStatement(statement);
        rootNode.currentGoStmt().addCreateProcStmt(statement);
        return statement;
    }


    @Override
    public Var visitProcedure_param(TSqlParser.Procedure_paramContext ctx) {
        String varName = ctx.LOCAL_ID().getText().toUpperCase();
        Var.DataType dataType = visitData_type(ctx.data_type());
//        Object value = ctx.default_value() == null ? null : ctx.default_value().getText();

        Var var = new Var(varName, null, dataType);
        if (null != ctx.default_value()) {
            try {
                Var v = (Var) visitDefault_value(ctx.default_value()).getVarValue();
                if (v.getDataType().equals(Var.DataType.STRING)) {
                    v.setVarValue(StrUtils.trimQuot(v.getVarValue().toString()));
                }
                var.setVarValue(visitDefault_value(ctx.default_value()).getVarValue());
                var.setDefault(true);
            } catch (ParseException e) {
                this.addException("Parse Error ", locate(ctx));
            }
        }
        if (null != ctx.OUT() || null != ctx.OUTPUT()) {
            var.setVarType(Var.VarType.OUTPUT);
        }
        if (null != ctx.READONLY()) {
            var.setReadonly(true);
        }
        if (null != ctx.CURSOR()) {
            //TODO do something for cursor
        }
        return var;
    }

    @Override
    public BaseStatement visitDeclare_table(TSqlParser.Declare_tableContext ctx) {
        String varName = ctx.LOCAL_ID().getText();
        Var var = new Var(varName, null, Var.DataType.TABLE);
        var.setValueType(Var.ValueType.TABLE);
        var.setVarValue(visitColumn_def_table_constraints(ctx.table_type_definition().column_def_table_constraints()));
        DeclareStatement declareStatement = new DeclareStatement();
        declareStatement.addDeclareVar(var);
        this.pushStatement(declareStatement);
        return declareStatement;
    }

    private Map<String, String> declares = new HashMap<>();

    @Override
    public BaseStatement visitDeclare_common(TSqlParser.Declare_commonContext ctx) {
        DeclareStatement declareStatement = new DeclareStatement();
        for (TSqlParser.Declare_localContext declareLocalContext : ctx.declare_local()) {
            Var var = visitDeclare_local(declareLocalContext);

            declareStatement.addDeclareVar(var);

        }
        this.pushStatement(declareStatement);
        return declareStatement;
    }

    /**
     * 处理普通类型declare
     */
    @Override
    public Var visitDeclare_local(TSqlParser.Declare_localContext ctx) {
        Var.DataType dataType = visitData_type(ctx.data_type());
        String varName = ctx.LOCAL_ID().getText();
        Var var = new Var(varName, null, dataType);
        if (null != declares.get(varName.toUpperCase())) {
            addException(new AlreadyDeclaredException(varName, locate(ctx)));
        }
        declares.put(varName.toUpperCase(), varName.toUpperCase());

        if (null != ctx.expression()) {
            visit(ctx.expression());
            var.setExpr(this.popStatement());
            var.setValueType(Var.ValueType.EXPRESSION);
        }
        return var;
    }

    @Override
    public Var.DataType visitData_type(TSqlParser.Data_typeContext ctx) {
        /*String dataType = ctx.getText().toUpperCase();
        if (null != ctx.BIGINT()) {
            return Var.DataType.LONG;
        } else if (null != ctx.INT() || ctx.TINYINT() != null) {
            return Var.DataType.INT;
        } else if (null != ctx.BINARY()) {
            return Var.DataType.BINARY;
        } else if (null != ctx.DATE()) {
            return Var.DataType.DATE;
        } else if (null != ctx.DATETIME()
                || null != ctx.DATETIME2()
                || null != ctx.DATETIMEOFFSET()
                || null != ctx.TIMESTAMP()) {
            return Var.DataType.DATETIME;
        } else if (null != ctx.CHAR() || null != ctx.TEXT() || null != ctx.NVARCHAR()) {
            return Var.DataType.STRING;
        } else if (null != ctx.FLOAT() || null != ctx.REAL()) {
            return Var.DataType.FLOAT;
        } else if (null != ctx.BIT()
                || null != ctx.XML()
                || null != ctx.IMAGE()
                || null != ctx.UNIQUEIDENTIFIER()
                || null != ctx.GEOGRAPHY()
                || null != ctx.GEOMETRY()
                || null != ctx.HIERARCHYID()) {
            addException(dataType, locate(ctx));
            return Var.DataType.NULL;
        } else if (null != ctx.MONEY()
                || null != ctx.DECIMAL()
                || null != ctx.NUMERIC()) {
            return Var.DataType.DOUBLE;
        } else {
            return Var.DataType.STRING;
        }
*/
        String dataType = ctx.getText().toUpperCase();
        if (dataType.contains("BIGINT")) {
            return Var.DataType.LONG;
        } else if (dataType.contains("INT")) {
            return Var.DataType.INT;
        } else if (dataType.contains("BINARY")) {
            return Var.DataType.BINARY;
        } else if (dataType.contains("DATETIME") || dataType.contains("TIMESTAMP")) {
            return Var.DataType.TIMESTAMP;
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
            addException(dataType, locate(ctx));
            return Var.DataType.NULL;
        } else if (dataType.contains("DECIMAL")) {
            return Var.DataType.FLOAT;
        } else if (dataType.contains("MONEY")
                || dataType.contains("NUMERIC")) {
            return Var.DataType.DOUBLE;
        } else {
            return Var.DataType.STRING;
        }

    }


    /**
     * simple_id
     * | DOUBLE_QUOTE_ID
     * | SQUARE_BRACKET_ID
     */
    @Override
    public String visitId(TSqlParser.IdContext ctx) {
        if (null != ctx.simple_id()) {
            return ctx.simple_id().getText().trim();
        }
        if (null != ctx.DOUBLE_QUOTE_ID()) { //双引号
            //return trim(ctx.DOUBLE_QUOTE_ID().getText());
            return ctx.DOUBLE_QUOTE_ID().getText();
        }
        if (null != ctx.SQUARE_BRACKET_ID()) {
            return trim(ctx.SQUARE_BRACKET_ID().getText());
        }
        return null;
    }

    private String trim(String str) {
        return str.substring(1, str.length() - 1);
    }

    @Override
    public BaseStatement visitSet_exprission(TSqlParser.Set_exprissionContext ctx) {
        String varName = ctx.LOCAL_ID().getText();
        visit(ctx.expression());

        Var var = new Var(varName, this.popStatement());
        var.setValueType(Var.ValueType.EXPRESSION);
        SetStatement statement = new SetStatement();
        statement.setVar(var);
        pushStatement(statement);
        return statement;
    }

    @Override
    public BaseStatement visitSet_assignment_operator_expression(TSqlParser.Set_assignment_operator_expressionContext ctx) {

        String varName = ctx.LOCAL_ID().getText().trim();
        AssignmentOp aop = visitAssignment_operator(ctx.assignment_operator());
        visit(ctx.expression());
        Var var = new Var(varName, this.popStatement());
        var.setValueType(Var.ValueType.EXPRESSION);
        SetStatement statement = new SetStatement();
        statement.setVar(var);
        statement.setAop(aop);
        this.pushStatement(statement);
        return statement;
    }

    @Override
    public AssignmentOp visitAssignment_operator(TSqlParser.Assignment_operatorContext ctx) {
        return AssignmentOp.createAop(ctx.getText());
    }

    @Override
    public TreeNode visitSet_cursor(TSqlParser.Set_cursorContext ctx) {
//        return super.visitSet_cursor(ctx);
        Cursor cursor = visitDeclare_set_cursor_common(ctx.declare_set_cursor_common());
        String cursorName = ctx.LOCAL_ID().getText();
        cursor.setName(cursorName);
        if (ctx.column_name_list() != null) {
            //TODO 是否需要判断只有为update时才有columnlist
            cursor.addUpdatableColumns(visitColumn_name_list(ctx.column_name_list()));
        }
        cursor.setUpdatable(ctx.UPDATE() != null);
        Var var = new Var(cursorName, cursor, Var.DataType.CURSOR);
        var.setValueType(Var.ValueType.CURSOR);
        SetStatement statement = new SetStatement();
        statement.setVar(var);
        pushStatement(statement);
        return statement;
    }

    @Override
    public Object visitSet_special(TSqlParser.Set_specialContext ctx) {
        return super.visitSet_special(ctx);
    }

    @Override
    public BaseStatement visitClose_cursor(TSqlParser.Close_cursorContext ctx) {
        BaseStatement statement = new CloseCursorStatement(visitCursor_name(ctx.cursor_name()), null != ctx.GLOBAL());
        pushStatement(statement);
        return statement;
    }

    @Override
    public BaseStatement visitDeallocate_cursor(TSqlParser.Deallocate_cursorContext ctx) {
        BaseStatement statement = new DeallocateCursorStatement(visitCursor_name(ctx.cursor_name()), null != ctx.GLOBAL());
        pushStatement(statement);
        return statement;
    }

    @Override
    public BaseStatement visitOpen_cursor(TSqlParser.Open_cursorContext ctx) {
        BaseStatement statement = new OpenCursorStatement(visitCursor_name(ctx.cursor_name()), null != ctx.GLOBAL());
        pushStatement(statement);
        return statement;
    }

    @Override
    public BaseStatement visitDeclare_cursor_statement(TSqlParser.Declare_cursor_statementContext ctx) {
//        String cursorName = ctx.cursor_name().getText();
        String cursorName = visitCursor_name(ctx.cursor_name());
        DeclareCursorStatement statement = new DeclareCursorStatement();
        Cursor cursor = new Cursor(cursorName);
        cursor.setGlobal(true); //declare cursor default true
        if (ctx.getChildCount() == 2) { //只有名字定义
            statement.setCursor(cursor);
            pushStatement(statement);
            return statement;
        }

        if (ctx.declare_set_cursor_common() != null) {
            cursor = visitDeclare_set_cursor_common(ctx.declare_set_cursor_common());
            cursor.setName(cursorName);
        } else {
            cursor.setInsensitive(ctx.INSENSITIVE() != null);
            cursor.setScoll(ctx.SCROLL() != null);
            if (ctx.select_statement() != null) {
                visitSelect_statement(ctx.select_statement());
            }

            cursor.setTreeNode(popStatement());
        }
        if (ctx.column_name_list() != null) {
            //TODO 是否需要判断只有为update时才有columnlist
            cursor.addUpdatableColumns(visitColumn_name_list(ctx.column_name_list()));
        }
        cursor.setUpdatable(ctx.UPDATE() != null);

        statement.setCursor(cursor);
        pushStatement(statement);
        return statement;
    }

    @Override
    public BaseStatement visitFetch_cursor_statement(TSqlParser.Fetch_cursor_statementContext ctx) {
        FetchCursorStatement statement = new FetchCursorStatement();
        if (ctx.NEXT() != null) {
            statement.setDirection(FetchCursorStatement.FetchDirection.NEXT);
        }
        if (ctx.PRIOR() != null) {
            statement.setDirection(FetchCursorStatement.FetchDirection.PRIOR);
        }
        if (ctx.FIRST() != null) {
            statement.setDirection(FetchCursorStatement.FetchDirection.FIRST);
        }
        if (ctx.LAST() != null) {
            statement.setDirection(FetchCursorStatement.FetchDirection.LAST);
        }
        if (ctx.ABSOLUTE() != null) {
            statement.setDirection(FetchCursorStatement.FetchDirection.ABSOLUTE);
        }
        if (ctx.RELATIVE() != null) {
            statement.setDirection(FetchCursorStatement.FetchDirection.RELATIVE);
        }
        if (ctx.expression() != null) {
            visit(ctx.expression());
            statement.setExpr(popStatement());
        }
        statement.setGlobal(null != ctx.GLOBAL());
        statement.setCursorName(visitCursor_name(ctx.cursor_name()));
        for (TerminalNode str : ctx.LOCAL_ID()) {
            statement.addIntoVarName(str.getText());
        }
        pushStatement(statement);
        return statement;
    }

    @Override
    public Cursor visitDeclare_set_cursor_common(TSqlParser.Declare_set_cursor_commonContext ctx) {
        Cursor cursor = new Cursor();
        cursor.setGlobal(null == ctx.LOCAL());
        cursor.setScoll(null != ctx.SCROLL());
        if (ctx.STATIC() != null) {
            cursor.setScoll(true);
            cursor.setDataMode(Cursor.DataMode.STATIC);
        }
        if (ctx.KEYSET() != null) {
            cursor.setScoll(true);
            cursor.setDataMode(Cursor.DataMode.KEYSET);
        }
        if (ctx.DYNAMIC() != null) {
            cursor.setDataMode(Cursor.DataMode.DYNAMIC);
            cursor.setScoll(true);
        }
        if (ctx.FAST_FORWARD().size() > 0 || ctx.FORWARD_ONLY().size() > 0) {
            if (cursor.isScoll()) {
                addException("Both SCROLL And FAST_FORWARD", locate(ctx));
            }
            cursor.setDataMode(Cursor.DataMode.FAST_FORWARD);
        }
        if (ctx.READ_ONLY() != null) {
            cursor.setConsistencyMode(Cursor.ConsistencyMode.READ_ONLY);
        }
        if (ctx.SCROLL_LOCKS() != null) {
            cursor.setConsistencyMode(Cursor.ConsistencyMode.SCROLL_LOCKS);
        }
        if (ctx.OPTIMISTIC() != null) {
            cursor.setConsistencyMode(Cursor.ConsistencyMode.OPTIMISTIC);
        }
        cursor.setTypeWarning(ctx.TYPE_WARNING() != null);
        visitSelect_statement(ctx.select_statement());
        cursor.setTreeNode(popStatement());//获取select statement
        return cursor;
    }


    @Override
    public String visitCursor_name(TSqlParser.Cursor_nameContext ctx) {
        return null != ctx.id() ? visitId(ctx.id()) : ctx.LOCAL_ID().getText();
    }


    private void printChildren(ParserRuleContext ctx) {
        int size = ctx.getChildCount();
        System.out.println("children size : " + size);
        for (int i = 0; i < size; i++) {
            System.out.println("child=" + ctx.getChild(i));
        }
    }

    @Override
    public Object visitBlock_statement(TSqlParser.Block_statementContext ctx) {
        BeginEndStatement beginEndCmd = new BeginEndStatement(TreeNode.Type.BEGINEND);
        if (ctx.sql_clauses() == null) {
            addException("empty begin end block", locate(ctx));
            pushStatement(beginEndCmd);
            return beginEndCmd;
        }
        visit(ctx.sql_clauses());
        addNode(beginEndCmd);
        pushStatement(beginEndCmd);
        return beginEndCmd;
    }


    @Override
    public Object visitIf_statement(TSqlParser.If_statementContext ctx) {
        IfStatement ifCmd = new IfStatement(TreeNode.Type.IF);
        visit(ctx.search_condition());
        LogicNode condition = (LogicNode) popStatement();
        ifCmd.setCondtion(condition);
        for (int i = 0; i < ctx.sql_clause().size(); i++) {
            visit(ctx.sql_clause(i));
            addNode(ifCmd);
        }
        pushStatement(ifCmd);
        return ifCmd;
    }

    private TreeNode pushStatement(TreeNode treeNode) {
        return this.curNodeStack.offerLast(treeNode) ? treeNode : null;
    }

    private TreeNode popStatement() {
        if (this.curNodeStack.isEmpty()) {
            return null;
        }
        return this.curNodeStack.pollFirst();
    }

    private List<TreeNode> popAll() {
        List<TreeNode> children = new ArrayList<TreeNode>();
        Iterator<TreeNode> iter = curNodeStack.iterator();
        while (iter.hasNext()) {
            children.add(popStatement());
        }
        return children;
    }

    private void addNode(TreeNode pNode) {
        Iterator<TreeNode> iter = curNodeStack.iterator();
        while (iter.hasNext()) {
            TreeNode node = this.popStatement();
            if (null == node) {
                continue;
            }
            pNode.addNode(node);
        }
    }

    private void addNodeAndPush(TreeNode pNode) {
        addNode(pNode);
        pushStatement(pNode);
    }

    @Override
    public LogicNode visitSearch_condition(TSqlParser.Search_conditionContext ctx) {
        List<TSqlParser.Search_condition_andContext> orList = ctx.search_condition_and();
        LogicNode orNode = new LogicNode(TreeNode.Type.OR);
        if (orList.size() == 1) {
            // LogicNode node = (LogicNode) visit(orList.get(0));
            // pushStatement(node);
            // return (LogicNode) visit(orList.get(0));
            visit(orList.get(0));
            LogicNode node = (LogicNode) popStatement();
            pushStatement(node);
            return node;
            // return null;
        } else {
            /*LogicNode andNode1 = (LogicNode) visit(orList.get(0));
            LogicNode andNode2 = (LogicNode) visit(orList.get(1));
            orNode.addNode(andNode1);
            orNode.addNode(andNode2);*/
            visit(orList.get(0));
            addNode(orNode);
            visit(orList.get(1));
            addNode(orNode);
            for (int i = 2; i < orList.size(); i++) {
                /*LogicNode andNode = (LogicNode) visit(orList.get(i));
                LogicNode subOrNode = orNode;
                orNode = new LogicNode(TreeNode.Type.OR);
                orNode.addNode(subOrNode);
                orNode.addNode(andNode);*/
                visit(orList.get(i));
                LogicNode subOrNode = orNode;
                orNode = new LogicNode(TreeNode.Type.OR);
                orNode.addNode(subOrNode);
                addNode(orNode);
            }
        }
        pushStatement(orNode);
        return orNode;
        // return visitChildren(ctx);
    }

    @Override
    public LogicNode visitSearch_condition_and(TSqlParser.Search_condition_andContext ctx) {
        LogicNode andNode = new LogicNode(TreeNode.Type.AND);
        List<TSqlParser.Search_condition_notContext> andList = ctx.search_condition_not();
        if (andList.size() == 1) {
            visit(andList.get(0));
            //return (LogicNode) visit(andList.get(0));
            return null;
        } else {
            /*LogicNode notNode1 = (LogicNode) visit(andList.get(0));
            LogicNode notNode2 = (LogicNode) visit(andList.get(1));
            andNode.addNode(notNode1);
            andNode.addNode(notNode2);*/
            visit(andList.get(0));
            addNode(andNode);
            visit(andList.get(1));
            addNode(andNode);
            for (int i = 2; i < andList.size(); i++) {
                visit(andList.get(i));
                LogicNode subAndNode = andNode;
                andNode = new LogicNode(TreeNode.Type.AND);
                andNode.addNode(subAndNode);
                addNode(andNode);
                /*LogicNode notNode = (LogicNode) visit(andList.get(i));
                LogicNode subAndNode = andNode;
                andNode = new LogicNode(TreeNode.Type.AND);
                andNode.addNode(subAndNode);
                andNode.addNode(notNode);*/
            }
        }
        pushStatement(andNode);
        return andNode;
        // return visitChildren(ctx);
    }

    @Override
    public LogicNode visitSearch_condition_not(TSqlParser.Search_condition_notContext ctx) {
        LogicNode notNode = new LogicNode(TreeNode.Type.NOT);
        TSqlParser.PredicateContext predicate = ctx.predicate();
        // TreeNode preNode = (TreeNode) visit(predicate);
        visit(predicate);
        if (ctx.NOT() != null) {
            // not
            notNode.setNot();
        }
        // notNode.addNode(preNode);
        addNode(notNode);
        pushStatement(notNode);
        return notNode;
        // return visitChildren(ctx);
    }

    @Override
    public LogicNode visitPredicate(TSqlParser.PredicateContext ctx) {
        PredicateNode predicateNode = new PredicateNode(TreeNode.Type.PREDICATE);
        if (ctx.EXISTS() != null) {
            predicateExists(ctx, predicateNode);
        } else if (ctx.comparison_operator() != null) {
            predicateComp(ctx, predicateNode);
        } else if (ctx.BETWEEN() != null) {
            predicateBetween(ctx, predicateNode);
        } else if (ctx.IN() != null) {
            predicateIn(ctx, predicateNode);
        } else if (ctx.LIKE() != null) {
            predicateLike(ctx, predicateNode);
        } else if (ctx.IS() != null) {
            predicateIs(ctx, predicateNode);
        } else if (ctx.search_condition() != null) {
            // LogicNode node = (LogicNode) visit(ctx.search_condition());
            visit(ctx.search_condition());
            LogicNode node = (LogicNode) popStatement();
            node.setPriority();
            pushStatement(node);
            return node;
        } else {
            // TODO implement DECIMAL
            addException("boolean expression DECIMAL", locate(ctx));
            return null;
        }
        pushStatement(predicateNode);
        return predicateNode;
        // return visitChildren(ctx);
    }

    private void predicateExists(TSqlParser.PredicateContext ctx, PredicateNode node) {
        node.setEvalType(PredicateNode.CompType.EXISTS);
        // BaseStatement expr = (BaseStatement) visit(ctx.subquery());
        // node.setExpr(expr);
        visit(ctx.subquery());
        addNode(node);
    }

    private void predicateComp(TSqlParser.PredicateContext ctx, PredicateNode node) {
        String op = ctx.comparison_operator().getText();
        node.setOp(op);
        List<TSqlParser.ExpressionContext> list = ctx.expression();
        if (list.size() < 1)
            return;
        for (TSqlParser.ExpressionContext exprCtx : list) {
            // BaseStatement expr = (BaseStatement) visit(exprCtx);
            // node.setExpr(expr);
            visit(exprCtx);
            addNode(node);
        }
        if (ctx.ALL() != null) {
            node.setEvalType(PredicateNode.CompType.COMPALL);
            visit(ctx.subquery());
        } else if (ctx.SOME() != null) {
            node.setEvalType(PredicateNode.CompType.COMPSOME);
            visit(ctx.subquery());
        } else if (ctx.ANY() != null) {
            node.setEvalType(PredicateNode.CompType.COMPANY);
            visit(ctx.subquery());
        } else {
            node.setEvalType(PredicateNode.CompType.COMP);
        }
        addNode(node);
    }

    private void predicateBetween(TSqlParser.PredicateContext ctx, PredicateNode node) {
        node.setEvalType(PredicateNode.CompType.BETWEEN);
        if (ctx.NOT() != null)
            node.setNotComp();
        List<TSqlParser.ExpressionContext> list = ctx.expression();
        if (list.size() != 3) {
            addException("BETWEEN expression count not equal 3", locate(ctx));
            return;
        }
        for (TSqlParser.ExpressionContext exprCtx : list) {
            // BaseStatement expr = (BaseStatement) visit(exprCtx);
            // node.setExpr(expr);
            visit(exprCtx);
            addNode(node);
        }
    }

    private void predicateIn(TSqlParser.PredicateContext ctx, PredicateNode node) {
        node.setEvalType(PredicateNode.CompType.IN);
        if (ctx.NOT() != null)
            node.setNotComp();
        List<TSqlParser.ExpressionContext> list = ctx.expression();
        if (list.size() != 1) {
            addException("IN expression count not equal 1", locate(ctx));
            return;
        }
        visit(list.get(0));
        addNode(node);
        if (ctx.subquery() != null) {
            node.setCompInQuery();
            visit(ctx.subquery());
            addNode(node);
        }
        if (ctx.expression_list() != null) {
            visit(ctx.expression_list());
            addNode(node);
        }
    }

    private void predicateLike(TSqlParser.PredicateContext ctx, PredicateNode node) {
        node.setEvalType(PredicateNode.CompType.LIKE);
        if (ctx.NOT() != null)
            node.setNotComp();
        List<TSqlParser.ExpressionContext> list = ctx.expression();
        if (list.size() < 2) {
            addException("LIKE expression count less than 2", locate(ctx));
            return;
        }
        for (TSqlParser.ExpressionContext exprCtx : list) {
            visit(exprCtx);
            addNode(node);
        }
    }

    private void predicateIs(TSqlParser.PredicateContext ctx, PredicateNode node) {
        node.setEvalType(PredicateNode.CompType.IS);
        if (ctx.null_notnull().NOT() != null) {
            node.setNotComp();
        }
        List<TSqlParser.ExpressionContext> list = ctx.expression();
        if (list.size() != 1) {
            addException("IS expression count not equal 1", locate(ctx));
            return;
        }
        visit(list.get(0));
        addNode(node);
    }

    @Override
    public Object visitWhile_statement(TSqlParser.While_statementContext ctx) {
        WhileStatement whileCmd = new WhileStatement(TreeNode.Type.WHILE);
        visit(ctx.search_condition());
        LogicNode condition = (LogicNode) popStatement();
        whileCmd.setCondtionNode(condition);
        visit(ctx.sql_clause());
        addNode(whileCmd);
        pushStatement(whileCmd);
        return whileCmd;
    }

    @Override
    public Object visitBreak_statement(TSqlParser.Break_statementContext ctx) {
        BreakStatement breakCmd = new BreakStatement(TreeNode.Type.BREAK);
        pushStatement(breakCmd);
        return breakCmd;
    }

    @Override
    public Object visitContinue_statement(TSqlParser.Continue_statementContext ctx) {
        ContinueStatement continueCmd = new ContinueStatement(TreeNode.Type.CONTINUE);
        pushStatement(continueCmd);
        return continueCmd;
    }

    @Override
    public Object visitReturn_statement(TSqlParser.Return_statementContext ctx) {
        ReturnStatement returnCmd = new ReturnStatement(TreeNode.Type.RETURN);
        if (ctx.expression() != null) {
            visit(ctx.expression());
            TreeNode expr = popStatement();
            returnCmd.setExpr(expr);
        }
        pushStatement(returnCmd);
        return returnCmd;
    }

    @Override
    public Object visitGoto_statement(TSqlParser.Goto_statementContext ctx) {
        GotoStatement gotoCmd = new GotoStatement(TreeNode.Type.GOTO);
        String label = ctx.id().getText();
        if (ctx.GOTO() != null) {
            gotoCmd.setAction();
        }
        // goSeq + label allow same label occured in different GO block
        gotoCmd.setLabel(rootNode.currentGoStmt().getGoSeq() + label);
        pushStatement(gotoCmd);
        if (gotoCmd.getAction())
            rootNode.currentGoStmt().addGotoAction(gotoCmd);
        else
            rootNode.currentGoStmt().addGotoLabel(gotoCmd);
        return gotoCmd;
    }

    @Override
    public Object visitPrint_statement(TSqlParser.Print_statementContext ctx) {
        PrintStatement printCmd = new PrintStatement(TreeNode.Type.PRINT);
        visit(ctx.expression());
        TreeNode expr = popStatement();
        printCmd.addExpression(expr);
        pushStatement(printCmd);
        return printCmd;
    }

    @Override
    public Object visitThrow_statement(TSqlParser.Throw_statementContext ctx) {
        ThrowStatement throwCmd = new ThrowStatement(TreeNode.Type.THROW);
        // THROW stmt without any args can only exists in CATCH stmt
        if (ctx.message == null && ctx.error_number == null && ctx.state == null) {
            throwCmd.setEmptyArg();
            pushStatement(throwCmd);
            rootNode.currentGoStmt().addThrowStmt(throwCmd);
            return throwCmd;
        }
        if (ctx.message == null || ctx.error_number == null || ctx.state == null)
            addException("throw stmt miss args", locate(ctx));
        throwCmd.setMsg(ctx.message.getText());
        throwCmd.setErrorNumStr(ctx.error_number.getText());
        throwCmd.setStateNumStr(ctx.state.getText());
        pushStatement(throwCmd);
        return throwCmd;
    }

    @Override
    public Object visitRaiseerror_statement(TSqlParser.Raiseerror_statementContext ctx) {
        RaiseStatement raiseCmd = new RaiseStatement(TreeNode.Type.RAISE);
        if (ctx.msg == null || ctx.severity == null || ctx.state == null) {
            addException("raise error stmt miss args", locate(ctx));
        }
        raiseCmd.setMsgStr(ctx.msg.getText());
        raiseCmd.setSeverityStr(ctx.severity.getText());
        raiseCmd.setStateStr(ctx.state.getText());
        List<TSqlParser.Constant_LOCAL_IDContext> list = ctx.constant_LOCAL_ID();
        for (int i = 2; i < list.size(); i++) {
            TSqlParser.Constant_LOCAL_IDContext localId = list.get(i);
            if (localId.constant() != null)
                raiseCmd.appendArg(visitConstant(localId.constant()));
            else {
                Var var = new Var();
                var.setDataType(Var.DataType.STRING);
                var.setVarValue(localId.LOCAL_ID().getText());
                raiseCmd.appendArg(var);
            }
        }
        pushStatement(raiseCmd);
        return raiseCmd;
    }

    @Override
    public Object visitTry_catch_statement(TSqlParser.Try_catch_statementContext ctx) {
        TryCatchStatement tryCatchCmd = new TryCatchStatement(TreeNode.Type.TRY);
        if (ctx.sql_clauses(0) != null) {
            visit(ctx.sql_clauses(0));
            addNode(tryCatchCmd);
        }
        if (ctx.sql_clauses(1) != null) {
            visit(ctx.sql_clauses(1));
            addNode(tryCatchCmd);
        }
        pushStatement(tryCatchCmd);
        return tryCatchCmd;
    }

    @Override
    public Object visitWaitfor_statement(TSqlParser.Waitfor_statementContext ctx) {
        WaitStatement waitStmt = new WaitStatement(TreeNode.Type.WAIT);
        if (ctx.DELAY() != null) {
            waitStmt.setDelay();
        }
        if (ctx.expression() != null) {
            visit(ctx.expression());
            TreeNode expr = popStatement();
            waitStmt.addExpr(expr);
        }
        pushStatement(waitStmt);
        return waitStmt;
    }


//=============================DDL==================================================


    /**
     * create_database
     * | create_index
     * | create_procedure
     * | create_statistics
     * | create_table
     * | create_type
     * | create_view
     * <p>
     * <p>
     * <p>
     * | drop_database
     * | drop_index
     * | drop_procedure
     * | drop_statistics
     * | drop_table
     * | drop_type
     * | drop_view
     *
     * @param ctx
     * @return *     create_database
     * | create_index
     * | create_procedure
     * | create_statistics
     * | create_table
     * | create_type
     * | create_view
     */

    @Override
    public Object visitAlter_database(TSqlParser.Alter_databaseContext ctx) {
        addException("ALTER DATABASE", locate(ctx));
        return null;
    }

    private Position locate(ParserRuleContext ctx) {
        return new Position(ctx.getStart().getLine(), ctx.getStart().getCharPositionInLine(),
                ctx.getStop().getLine(), ctx.getStop().getCharPositionInLine());
    }


    @Override
    public SqlStatement visitCreate_database(TSqlParser.Create_databaseContext ctx) {
        SqlStatement sqlStatement = new SqlStatement(Common.CREATE_DATA_BASE);
        StringBuffer sql = new StringBuffer();
        sql.append(ctx.CREATE().getText()).append(Common.SPACE);
        sql.append(ctx.DATABASE().getText()).append(Common.SPACE);
        String databaseName = visitId(ctx.id(0));
        checkLength(databaseName, 128, "database name ");
        sql.append(databaseName);
        if (ctx.CONTAINMENT() != null) {
            addException(ctx.CONTAINMENT().getText(), locate(ctx));
        }
        if (ctx.ON(0) != null) {
            addException(ctx.ON(0).getText(), locate(ctx));
        }
        if (ctx.LOG() != null) {
            addException(ctx.LOG().getText(), locate(ctx));
        }
        if (ctx.COLLATE() != null) {
            addException(ctx.COLLATE().getText(), locate(ctx));
        }
        if (ctx.WITH() != null) {
            addException(ctx.WITH().getText(), locate(ctx));
        }
        if (ctx.FOR() != null) {
            addException(ctx.FOR().getText(), locate(ctx));
        }
        sqlStatement.setSql(sql.toString());
        pushStatement(sqlStatement);
        return sqlStatement;
    }

    @Override
    public SqlStatement visitCreate_index(TSqlParser.Create_indexContext ctx) {
        /**
         *  : CREATE UNIQUE? clustered? INDEX id ON table_name_with_hint '(' column_name_list (ASC | DESC)? ')'
         (index_options)?
         (ON id)?
         ';'?
         */
        CreateIndexStatement createIndexStatement = new CreateIndexStatement("CREATE INDEX");
        if (null != ctx.UNIQUE()) {
            addException("CREATE UNIQUE INDEX", locate(ctx));
        }
        if (null != ctx.clustered()) {
            addException("CREATE clustered INDEX", locate(ctx));
        }
        if (null != ctx.ASC() || null != ctx.DESC()) {
            addException("CREATE  INDEX  ON ASC,DESC", locate(ctx));
        }
        if (null != ctx.index_options()) {
            addException("CREATE  INDEX  with [ " + ctx.index_options().getText() + "]", locate(ctx));
        }
        createIndexStatement.setCols(visitColumn_name_list(ctx.column_name_list()));
        createIndexStatement.setIndexName(visitId(ctx.id(0)));
        createIndexStatement.setTableName(visitTable_name_with_hint(ctx.table_name_with_hint()));
        pushStatement(createIndexStatement);
        return createIndexStatement;
    }

    @Override
    public FuncName visitFunc_proc_name(TSqlParser.Func_proc_nameContext ctx) {
        FuncName fn = new FuncName();
        if (ctx.database != null) {
            fn.setDatabase(visitId(ctx.database));
        }
        if (ctx.schema != null) {
            fn.setSchema(visitId(ctx.schema));
        }
        if (null != ctx.procedure) {
            fn.setFuncName(visitId(ctx.procedure));
        }
        if (null != ctx.LOCAL_ID()) {
            fn.setFuncName(ctx.LOCAL_ID().getText().trim());
            fn.setVariable(true);
        }

        return fn;
    }


    @Override
    public Var visitDefault_value(TSqlParser.Default_valueContext ctx) {
        Var var = new Var();
        if (null != ctx.NULL() || null != ctx.DEFAULT()) {
            var.setDataType(Var.DataType.NULL);
//            var.setVarValue(Common.NULL);
            var.setVarValue(null);
        }
        if (null != ctx.constant()) {
            var = visitConstant(ctx.constant());
        }
        return var;
    }

    @Override
    public Var visitConstant(TSqlParser.ConstantContext ctx) {
        Var var = new Var();
        StringBuffer sb = new StringBuffer();
        if (null != ctx.STRING()) {
            String s = ctx.STRING().getText();
            if (s.startsWith("N")) {
                s = s.substring(1);
            }
            sb.append(s).append(Common.SPACE);
            var.setDataType(Var.DataType.STRING);
            var.setVarValue(sb.toString());
        } else if (null != ctx.BINARY()) {
            sb.append(ctx.STRING().getText());
            var.setDataType(Var.DataType.BINARY);
            var.setVarValue(sb.toString());
        } else {
            var.setDataType(Var.DataType.FLOAT);
            if (null != ctx.sign()) {
                sb.append(ctx.sign().getText());
            }
            if (null != ctx.DECIMAL()) {
                sb.append(ctx.DECIMAL().getText());
                var.setDataType(Var.DataType.LONG);
            }
            if (null != ctx.REAL()) {
                sb.append(ctx.REAL().getText());
            }
            if (null != ctx.FLOAT()) {
                sb.append(ctx.FLOAT().getText());
            }
            //sign? dollar='$' (DECIMAL | FLOAT)       // money 暂时按float类型处理
            var.setVarValue(sb.toString());
        }
        return var;
    }


    @Override
    public FuncName visitTable_name(TSqlParser.Table_nameContext ctx) {
        String database = "";
        String funcName = "";
        String schema = "";
        if (ctx.database != null) {
            database = visitId(ctx.database);
        }
        if (ctx.schema != null) {
            schema = visitId(ctx.schema);
        }
        funcName = visitId(ctx.table);
        FuncName fn = new FuncName(database, funcName, schema);
        tableNameList.add(fn.getRealFullFuncName());
        return fn;
    }


    @Override
    public SqlStatement visitCreate_statistics(TSqlParser.Create_statisticsContext ctx) {
        addException("create statistics", locate(ctx));
        return null;
    }


    @Override
    public SqlStatement visitCreate_table(TSqlParser.Create_tableContext ctx) {
        String tableName = visitTable_name(ctx.table_name()).getRealFullFuncName();
        checkLength(tableName, 128, "table name ");
        CreateTableStatement createTableStatement = new CreateTableStatement(tableName);
        String columnDefs = visitColumn_def_table_constraints(ctx.column_def_table_constraints());
        createTableStatement.setColumnDefs(columnDefs);
        if (ctx.crud_table() != null) {
            createTableStatement.setCrudStr(visitCrud_table(ctx.crud_table()));
        }
        if (procFlag && ctx.crud_table() == null) {
            String column = columnDefs.contains(",") ? columnDefs.split(",")[0] : columnDefs;
            column = column.split(" ")[0];
            createTableStatement.setCrudStr(String.format(Common.crudStr, column));
        }
        if (null != ctx.ON() || null != ctx.TEXTIMAGE_ON()) {
            addException(" filegroup", locate(ctx));
        }
        pushStatement(createTableStatement);
        return createTableStatement;
    }


    @Override
    public String visitColumn_def_table_constraints(TSqlParser.Column_def_table_constraintsContext ctx) {
        StringBuilder sb = new StringBuilder();
        if (!ctx.column_def_table_constraint().isEmpty()) {
            for (int i = 0; i < ctx.column_def_table_constraint().size(); i++) {
                if (i != 0) {
                    sb.append(",");
                }
                sb.append(visitColumn_def_table_constraint(ctx.column_def_table_constraint(i)));
            }
        }
        return sb.toString();
    }

    @Override
    public String visitColumn_def_table_constraint(TSqlParser.Column_def_table_constraintContext ctx) {
        StringBuffer sb = new StringBuffer();
        if (null != ctx.table_constraint()) {
            sb.append(visitTable_constraint(ctx.table_constraint()));
        }
        if (null != ctx.column_definition()) {
            sb.append(visitColumn_definition(ctx.column_definition()));
        }
        return sb.toString();
    }

    @Override
    public String visitColumn_definition(TSqlParser.Column_definitionContext ctx) {
        StringBuffer sb = new StringBuffer();
        if (null != ctx.COLLATE()) {
            addException(ctx.COLLATE().getText(), locate(ctx));
        }
        if (null != ctx.CONSTRAINT()) {
            addException(ctx.CONSTRAINT().getText(), locate(ctx));
        }
        if (null != ctx.IDENTITY()) {
            addException(ctx.IDENTITY().getText(), locate(ctx));
        }
        sb.append(StrUtils.replaceAllBracketToQuit(visitId(ctx.id(0))));
        sb.append(Common.SPACE);
        if (null != ctx.data_type()) {
            sb.append(formatDataType(ctx.data_type())).append(Common.SPACE);
        }
        //TODO : id (data_type | AS expression) (COLLATE id)? null_notnull?  expression not impl
        if (!ctx.column_constraint().isEmpty()) {
            sb.append(visitColumn_constraint(ctx.column_constraint(0)));
        }
        return sb.toString();
    }

    private String formatDataType(TSqlParser.Data_typeContext data_typeContext) {
        String dataType = data_typeContext.start.getInputStream().getText(
                new Interval(data_typeContext.start.getStartIndex(), data_typeContext.stop.getStopIndex())).toUpperCase();
        if (dataType.contains("XML")
                || dataType.contains("IMAGE")
                || dataType.contains("UNIQUEIDENTIFIER")
                || dataType.contains("GEOGRAPHY")
                || dataType.contains("GEOMETRY")
                || dataType.contains("HIERARCHYID")
                || dataType.contains("SQL_VARIANT")) {
            addException(dataType, locate(data_typeContext));
            return "";
        } else if (dataType.contains("BIT")) {
            return "BOOLEAN";

        } else if (dataType.contains("FLOAT") || dataType.contains("REAL")) {
            return "FlOAT";
        } else if (dataType.contains("MONEY")) {
            return "DOUBLE";
        } else if (dataType.contains("NUMERIC")) {
            return "DOUBLE";
        } else if (dataType.contains("TIMESTAMP") || dataType.contains("DATETIME")) {
            return "TIMESTAMP";
        } else if (dataType.contains("DATE") || dataType.contains("TIME")) {
            return "DATE";
        } else if (dataType.contains("BINARY")) {
            return "BINARY";
        } else if (dataType.contains("TEXT") || dataType.contains("MAX")) {
            return "STRING";
        } else if (dataType.contains("NCHAR")) {
            dataType = dataType.replaceAll("NCHAR", "CHAR");
            return StrUtils.trimBracket(dataType);
        } else if (dataType.contains("NVARCHAR")) {
            String d = dataType.replaceAll("NVARCHAR", "VARCHAR");
            return StrUtils.replaceAllBracket(d);
        } else if (dataType.contains("INT")) {
            return "INT";
        } else {
            return StrUtils.replaceAllBracket(dataType);
        }
    }

    @Override
    public String visitColumn_constraint(TSqlParser.Column_constraintContext ctx) {
        if (null != ctx.PRIMARY() || null != ctx.CHECK() || null != ctx.UNIQUE()) {
            addException(" PRIMARY KEY,CHECK,UNIQUE", locate(ctx));
        }
        return "";
    }

    @Override
    public String visitTable_constraint(TSqlParser.Table_constraintContext ctx) {
        addException("PRIMARY KEY", locate(ctx));
        return "";
    }

    @Override
    public SqlStatement visitCreate_type(TSqlParser.Create_typeContext ctx) {
        SqlStatement rs = new SqlStatement();
        addException("create type", locate(ctx));
        return rs;
    }

    @Override
    public String visitSimple_name(TSqlParser.Simple_nameContext ctx) {
        StringBuffer sb = new StringBuffer();
        if (!ctx.id().isEmpty()) {
            int size = ctx.id().size();
            if (size > 1) {
                /*sb.append(visitId(ctx.id(0)));
                sb.append(".");*/
                sb.append(visitId(ctx.id(1)));
            } else {
                sb.append(visitId(ctx.id(0)));
            }
        }
        return sb.toString();
    }


    @Override
    public SqlStatement visitCreate_view(TSqlParser.Create_viewContext ctx) {
        CreateViewStatement createViewStatement = new CreateViewStatement(Common.CREATE_VIEW);
        StringBuffer sql = new StringBuffer();
        sql.append(ctx.CREATE().getText());
        sql.append(Common.SPACE);
        sql.append(ctx.VIEW());
        sql.append(Common.SPACE);
        String viewName = visitSimple_name(ctx.simple_name());
        checkLength(viewName, 128, "view name ");
        sql.append(viewName).append(Common.SPACE);
        if (null != ctx.column_name_list()) {
            sql.append("(").append(StrUtils.concat(visitColumn_name_list(ctx.column_name_list())));
            sql.append(")");
        }
        if (!ctx.view_attribute().isEmpty()) {
            addException(" ENCRYPTION | SCHEMABINDING | VIEW_METADATA ", locate(ctx));
        }
        sql.append(Common.SPACE);
        sql.append(ctx.AS().getText());
        sql.append(Common.SPACE);
        sql.append(visitSelect_statement(ctx.select_statement()).getSql());
        popStatement();
        if (null != ctx.OPTION()) {
            addException("WITH CHECK OPTION", locate(ctx));
        }
        createViewStatement.setSql(sql.toString());
        createViewStatement.addTableNames(tableNameList);
        pushStatement(createViewStatement);
        return createViewStatement;
    }

    @Override
    public List<String> visitColumn_name_list(TSqlParser.Column_name_listContext ctx) {
        List<String> columns = new ArrayList<>();
        for (TSqlParser.IdContext idContext : ctx.id()) {
            columns.add(StrUtils.replaceAllBracketToQuit(visitId(idContext)));
        }
        return columns;
    }


    @Override
    public SqlStatement visitAlter_table(TSqlParser.Alter_tableContext ctx) {
        AlterTableStatement alterTableStatement = new AlterTableStatement();
        StringBuffer sql = new StringBuffer();
        sql.append(ctx.ALTER(0).getText()).append(Common.SPACE);
        sql.append(ctx.TABLE(0).getText()).append(Common.SPACE);
        sql.append(visitTable_name(ctx.table_name(0)).getRealFullFuncName());
        if (null != ctx.SET()) {
            addException("SET   LOCK_ESCALATION ", locate(ctx));
        }

        if (null != ctx.DROP() && null != ctx.CONSTRAINT()) {
            addException("DROP CONSTRAINT", locate(ctx));
        }
        if (null != ctx.WITH()) {
            addException("WITH CHECK ADD CONSTRAINT", locate(ctx));
        }
        if (null != ctx.CHECK()) {
            addException("CHECK CONSTRAINT", locate(ctx));
        }
        /*/
        | DROP CONSTRAINT ( IF EXISTS )?  constraint=id
                             | DROP  COLUMN  ( IF EXISTS )?  id (',' id)*
                             | ALTER COLUMN column_def_table_constraint
        */

        if (null != ctx.DROP() && null != ctx.id() && ctx.id().size() >= 0) {
            if (ctx.id().size() > 1) {
                addException("Just supported delete a column ", locate(ctx));
            } else {
                sql.append(Common.SPACE).append("DROP")
                        .append(" COLUMN ");
                sql.append(ctx.id(0).getText());
            }
        }

        if (null != ctx.ADD() && null != ctx.column_def_table_constraint()) {
            sql.append(Common.SPACE).append(ctx.ADD().getText()).append(Common.SPACE);
            sql.append(" COLUMNS ");
            sql.append("(");
            sql.append(visitColumn_def_table_constraint(ctx.column_def_table_constraint()));
            sql.append(")");
        }
        if (ctx.ALTER().size() >= 2 && null != ctx.column_def_table_constraint()) {
            sql.append(Common.SPACE).append(" CHANGE ").append(Common.SPACE);
            sql.append(ctx.column_def_table_constraint().column_definition().id(0).getText()).append(Common.SPACE);
            sql.append(visitColumn_def_table_constraint(ctx.column_def_table_constraint()));
        }
        alterTableStatement.setSql(sql.toString());
        alterTableStatement.addTables(tableNameList);

        pushStatement(alterTableStatement);
        return alterTableStatement;
    }

    @Override
    public SqlStatement visitShow_tables(TSqlParser.Show_tablesContext ctx) {
        SqlStatement sqlStatement = new SqlStatement(Common.SHOW_TABLES);
        StringBuffer sql = new StringBuffer();
        sql.append(ctx.SHOW().getText()).append(Common.SPACE);
        sql.append(ctx.TABLES().getText()).append(Common.SPACE);
        sqlStatement.setSql(sql.toString());
        sqlStatement.setAddResult(true);
        pushStatement(sqlStatement);
        return sqlStatement;
    }

    @Override
    public SqlStatement visitShow_databases(TSqlParser.Show_databasesContext ctx) {
        SqlStatement sqlStatement = new SqlStatement(Common.SHOW_TABLES);
        StringBuffer sql = new StringBuffer();
        sql.append(ctx.SHOW().getText()).append(Common.SPACE);
        sql.append(ctx.DATABASES().getText()).append(Common.SPACE);
        sqlStatement.setSql(sql.toString());
        sqlStatement.setAddResult(true);
        pushStatement(sqlStatement);
        return sqlStatement;
    }

    @Override
    public SqlStatement visitTruncate_table(TSqlParser.Truncate_tableContext ctx) {
        String tableName = visitTable_name(ctx.table_name()).getRealFullFuncName();
        TruncateTableStatement truncateTableStatement = new TruncateTableStatement(tableName);
        truncateTableStatement.setAddResult(false);
        pushStatement(truncateTableStatement);
        return truncateTableStatement;
    }

    @Override
    public SqlStatement visitDrop_database(TSqlParser.Drop_databaseContext ctx) {
        SqlStatement rs = new SqlStatement(Common.DROP_DATA_BASE);
        StringBuffer sql = new StringBuffer();
        if (!ctx.id().isEmpty()) {
            for (int i = 0; i < ctx.id().size(); i++) {
                if (i != 0) {
                    sql.append(";");
                }
                sql.append(ctx.DROP().getText()).append(Common.SPACE);
                sql.append(ctx.DATABASE().getText()).append(Common.SPACE);
                sql.append(visitId(ctx.id(i)));
            }
        }
        rs.setAddResult(false);
        rs.setSql(sql.toString());
        pushStatement(rs);
        return rs;
    }

    @Override
    public SqlStatement visitDrop_index(TSqlParser.Drop_indexContext ctx) {
        addException("drop index ", locate(ctx));
        return null;
    }


    @Override
    public BaseStatement visitDrop_procedure(TSqlParser.Drop_procedureContext ctx) {
        //    : DROP PROCEDURE (IF EXISTS)? func_proc_name ';'?
        SqlStatement rs = new SqlStatement();
        StringBuffer sql = new StringBuffer();
        sql.append(ctx.DROP().getText()).append(Common.SPACE);
        sql.append("proc").append(Common.SPACE);
        if (null != ctx.IF()) {
            sql.append(ctx.IF().getText()).append(Common.SPACE);
        }
        if (null != ctx.EXISTS()) {
            sql.append(ctx.EXISTS().getText()).append(Common.SPACE);
        }
        List<FuncName> funcNames = new ArrayList<>();
        if (!ctx.func_proc_name().isEmpty()) {
            for (int i = 0; i < ctx.func_proc_name().size(); i++) {
                if (i != 0) {
                    sql.append(Common.SPACE).append(",");
                }
                FuncName funcName = visitFunc_proc_name(ctx.func_proc_name(i));
                sql.append(funcName.getRealFullFuncName());
                funcNames.add(funcName);
            }
        }
        rs.setSql(sql.toString());
        DropProcedureStatement statement = new DropProcedureStatement(funcNames);
        pushStatement(statement);

        return statement;
    }


    @Override
    public Object visitDrop_statistics(TSqlParser.Drop_statisticsContext ctx) {
        //SqlStatement rs = new SqlStatement(Common.DROP_STATISTICS);
        addException("drop statistics", locate(ctx));
        // pushStatement(rs);
        return null;
    }

    @Override
    public Object visitDrop_type(TSqlParser.Drop_typeContext ctx) {
        addException("drop type", locate(ctx));
        return null;
    }

    @Override
    public SqlStatement visitDrop_table(TSqlParser.Drop_tableContext ctx) {
        DropTableStatement dropTableStatement = new DropTableStatement(Common.DROP_TABLE);
        StringBuffer sql = new StringBuffer();
        sql.append(ctx.DROP().getText()).append(Common.SPACE);
        sql.append(ctx.TABLE().getText()).append(Common.SPACE);
        if (null != ctx.IF()) {
            sql.append(ctx.IF().getText()).append(Common.SPACE);
        }
        if (null != ctx.EXISTS()) {
            sql.append(ctx.EXISTS().getText()).append(Common.SPACE);
        }
        List<String> tableNames = new ArrayList<>();
        if (!ctx.table_name().isEmpty()) {
            for (int i = 0; i < ctx.table_name().size(); i++) {
                String tabName = visitTable_name(ctx.table_name(i)).getRealFullFuncName();
                tableNames.add(tabName);
            }
        }
        dropTableStatement.setSql(sql.toString());
        dropTableStatement.setTableName(tableNames);
        pushStatement(dropTableStatement);
        return dropTableStatement;
    }


    @Override
    public SqlStatement visitDrop_view(TSqlParser.Drop_viewContext ctx) {
        DropViewStatement rs = new DropViewStatement(Common.DROP_VIEW);
        StringBuffer sql = new StringBuffer();
        sql.append(ctx.DROP().getText()).append(Common.SPACE);
        sql.append(ctx.VIEW().getText()).append(Common.SPACE);
        if (null != ctx.IF()) {
            sql.append(ctx.IF().getText()).append(Common.SPACE);
        }
        if (null != ctx.EXISTS()) {
            sql.append(ctx.EXISTS().getText()).append(Common.SPACE);
        }

        List<String> tableNames = new ArrayList<>();
        if (!ctx.simple_name().isEmpty()) {
            for (int i = 0; i < ctx.simple_name().size(); i++) {
                String tabName = visitSimple_name(ctx.simple_name(i));
                tableNames.add(tabName);
            }
        }
        rs.setTableName(tableNames);
        rs.setSql(sql.toString());
        pushStatement(rs);
        return rs;
    }


    //=======================DML==============================
    /**
     * with_expression 表达式中存储为一个map
     * 如
     * WITH Sales_CTE (SalesPersonID, NumberOfOrders)
     * AS
     * (
     * SELECT SalesPersonID, COUNT(*)
     * FROM Sales.SalesOrderHeader
     * WHERE SalesPersonID IS NOT NULL
     * GROUP BY SalesPersonID
     * )
     * ======= key= Sales_CTE ，value="select --"
     */
    private HashMap<String, String> withExpressionSqlMap = new HashMap<String, String>();

    //=============================Insert_statement====================


    private void cleanTableVariable() {
        //tableNameList.clear();
        //localIdVariable.clear();
    }

    @Override
    public SqlStatement visitInsert_statement(TSqlParser.Insert_statementContext ctx) {
        InsertStatement insertStatement = new InsertStatement(Common.INSERT);
        cleanTableVariable();
        StringBuffer sql = new StringBuffer();
        if (null != ctx.with_expression()) {
            sql.append(visitWith_expression(ctx.with_expression()).getSql());
        }
        if (null != ctx.TOP()) {
            LimitStatement limitStatement = new LimitStatement();
            limitStatement.setNodeType(TreeNode.Type.LIMIT_NUMBER);
            visit(ctx.expression());
            limitStatement.setLimitValueNode(popStatement());
            insertStatement.addInsertValuesNode(limitStatement);
            if (null != ctx.PERCENT()) {
                limitStatement.setNodeType(TreeNode.Type.LIMIT_PERCENT);
                //rs.sql.append(ctx.PERCENT().getText()).append(Common.SPACE);
                addException("PERCENT", locate(ctx));
            }
        }

        sql.append("Insert into ").append(Common.SPACE);
        if (null != ctx.ddl_object()) {
            sql.append(visitDdl_object(ctx.ddl_object())).append(Common.SPACE);
        }
        if (null != ctx.rowset_function_limited()) {
            visitRowset_function_limited(ctx.rowset_function_limited());
        }
        if (null != ctx.insert_with_table_hints()) {
            visitInsert_with_table_hints(ctx.insert_with_table_hints());
        }

        if (null != ctx.column_name_list()) {
            sql.append("(");
            sql.append(StrUtils.concat(visitColumn_name_list(ctx.column_name_list())));
            sql.append(")");
        }
        if (null != ctx.output_clause()) {
            visitOutput_clause(ctx.output_clause());
        }
        TreeNode insertValuesNode = visitInsert_statement_value(ctx.insert_statement_value());
        if (null != ctx.for_clause()) {
            insertStatement.setForClause(visitFor_clause(ctx.for_clause()));
        }
        if (null != ctx.option_clause()) {
            visitOption_clause(ctx.option_clause());
        }
        insertStatement.setSql(sql.toString());
        insertStatement.addTableNames(tableNameList);
        insertStatement.addVariables(localIdVariable);
        insertStatement.addInsertValuesNode(insertValuesNode);
        pushStatement(insertStatement);
        cleanTableVariable();
        return insertStatement;

    }

    @Override
    public SqlStatement visitInsert_statement_value(TSqlParser.Insert_statement_valueContext ctx) {
        InsertStatementValueStatement rs = new InsertStatementValueStatement();
        StringBuffer sql = new StringBuffer();
        if (null != ctx.table_value_constructor()) {
            rs.setNodeType(TreeNode.Type.TABLE_VALUE);
            sql.append(visitTable_value_constructor(ctx.table_value_constructor()));
        }
        if (null != ctx.derived_table()) {
            rs.setNodeType(TreeNode.Type.DERIVED_TABLE);
            sql.append(visitDerived_table(ctx.derived_table()));
        }
        if (null != ctx.execute_statement()) {
            rs.setNodeType(TreeNode.Type.EXECUTE_STATEMENT);
            visit(ctx.execute_statement());
            addNode(rs);
        }
        if (null != ctx.DEFAULT()) {
            addException(ctx.DEFAULT().getText(), locate(ctx));
        }
        rs.setSql(sql.toString());
        return rs;

    }

//    private String getExpressionListSql(List<TreeNode> list) {
//        StringBuffer sb = new StringBuffer();
//        if (null == list || list.isEmpty()) {
//            return "";
//        }
//        for (TreeNode node : list) {
//            if (sb.length() > 0) {
//                sb.append(",");
//            }
//            sb.append(node.getSql()).append(Common.SPACE);
//        }
//        return sb.toString();
//    }


    private String getExpressionSql(TSqlParser.ExpressionContext expression) {
        TreeNode sqlTreeNode = (TreeNode) visit(expression);
        return sqlTreeNode.getSql().toString();
    }

    /**
     * insert_statement_value
     * : table_value_constructor
     * | derived_table
     * | execute_statement
     * | DEFAULT VALUES
     */
    @Override
    public String visitTable_value_constructor(TSqlParser.Table_value_constructorContext ctx) {
        StringBuffer sb = new StringBuffer();
        sb.append(ctx.VALUES().getText()).append(Common.SPACE);
        for (int i = 0; i < ctx.expression_list().size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append("(");
            sb.append(visitExpression_list(ctx.expression_list(i)).getSql());
            popStatement();
            sb.append(")");
        }
        return sb.toString();
    }

    //=============================update_statement====================
    @Override
    public SqlStatement visitUpdate_statement(TSqlParser.Update_statementContext ctx) {
        UpdateStatement updateStatement = new UpdateStatement(Common.UPDATE);
        StringBuffer sql = new StringBuffer();
        cleanTableVariable();
        withFlag = true;
        if (null != ctx.with_expression()) {
            visitWith_expression(ctx.with_expression());
        }
        sql.append(ctx.UPDATE().getText()).append(Common.SPACE);
        if (null != ctx.TOP()) {
            LimitStatement limitStatement = new LimitStatement();
            limitStatement.setNodeType(TreeNode.Type.LIMIT_NUMBER);
            visit(ctx.expression());
            limitStatement.setLimitValueNode(popStatement());
            updateStatement.addUpdateValuesNode(limitStatement);
            if (null != ctx.PERCENT()) {
                limitStatement.setNodeType(TreeNode.Type.LIMIT_PERCENT);
                addException("PERCENT", locate(ctx));
            }
        }
        if (null != ctx.ddl_object()) {
            sql.append(visitDdl_object(ctx.ddl_object()));
        }
        if (null != ctx.rowset_function_limited()) {
            visitRowset_function_limited(ctx.rowset_function_limited());
        }
        if (null != ctx.with_table_hints()) {
            visitWith_table_hints(ctx.with_table_hints());
        }
        sql.append(Common.SPACE).append(ctx.SET().getText()).append(Common.SPACE);
        for (int i = 0; i < ctx.update_elem().size(); i++) {
            if (i != 0) {
                sql.append(",");
            }
            sql.append(visitUpdate_elem(ctx.update_elem(i)));
        }
        if (null != ctx.output_clause()) {
            visitOutput_clause(ctx.output_clause());
        }

        if (null != ctx.FROM()) {
            sql.append(Common.SPACE).append(ctx.FROM().getText()).append(Common.SPACE);
            sql.append(visitTable_sources(ctx.table_sources()));
        }
        if (null != ctx.WHERE()) {
            sql.append(Common.SPACE).append(ctx.WHERE().getText()).append(Common.SPACE);
            if (null != ctx.search_condition_list()) {
                sql.append(visitSearch_condition_list(ctx.search_condition_list()));
            }
        }
        if (null != ctx.CURRENT()) {
            //TODO 在updatastatment上挂游标statement
            addException("cursor", locate(ctx));
        }
        if (null != ctx.for_clause()) {
            updateStatement.setForClause(visitFor_clause(ctx.for_clause()));
        }
        if (null != ctx.option_clause()) {
            visitOption_clause(ctx.option_clause());
        }
        cleanTableVariable();
        updateStatement.setSql(sql.toString());
        updateStatement.addTableNames(tableNameList);
        updateStatement.addVariables(localIdVariable);
        pushStatement(updateStatement);
        return updateStatement;

    }

    /**
     * rowset_function_limited
     * : openquery
     * | opendatasource
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public String visitRowset_function_limited(TSqlParser.Rowset_function_limitedContext ctx) {
        addException("open query  or openDatasource ", locate(ctx));
        return "";
    }


    @Override
    public String visitUpdate_elem(TSqlParser.Update_elemContext ctx) {
        StringBuffer sb = new StringBuffer();
        if (null != ctx.full_column_name()) {
            sb.append(visitFull_column_name(ctx.full_column_name()).getSql());
        }
        if (null != ctx.LOCAL_ID()) {
            sb.append(ctx.LOCAL_ID().getText());
        }
        if (null != ctx.assignment_operator()) {
            sb.append(ctx.assignment_operator()).append(Common.SPACE);

        } else {
            sb.append("=").append(Common.SPACE);
        }
        if (null != ctx.expression()) {
            TreeNode sqlStatement = (TreeNode) visit(ctx.expression());
            sb.append(sqlStatement.getSql());
            popStatement();
        } else {
            sb.append(ctx.udt_column_name.getText()).append(".").append(ctx.method_name.getText()).append("(");
            sb.append(visitExpression_list(ctx.expression_list()));
            sb.append(")");
        }
        return sb.toString();
    }


    //=============================del_statement====================

    /**
     * 思路：(1)先把sql解析为 delete (top 2) from table where ----
     * (2) 在spark的解析文件中新增 关键字top，使sql解析通过
     * (3) 把sql转化为 insert into table select ------,vid from table limt 2
     *
     * @param ctx
     * @return
     */

    @Override
    public SqlStatement visitDelete_statement(TSqlParser.Delete_statementContext ctx) {
        DeleteStatement deleteStatement = new DeleteStatement(Common.DELETE);
        cleanTableVariable();
        withFlag = true;
        StringBuffer sql = new StringBuffer();
        if (null != ctx.with_expression()) {
            visitWith_expression(ctx.with_expression());
        }

        sql.append(ctx.DELETE().getText()).append(Common.SPACE);
        if (null != ctx.TOP()) {
            LimitStatement limitStatement = new LimitStatement();
            limitStatement.setNodeType(TreeNode.Type.LIMIT_NUMBER);
            visit(ctx.expression());
            limitStatement.setLimitValueNode(popStatement());
            deleteStatement.addDelValuesNode(limitStatement);
            if (null != ctx.PERCENT()) {
                limitStatement.setNodeType(TreeNode.Type.LIMIT_PERCENT);
                //rs.sql.append(ctx.PERCENT().getText()).append(Common.SPACE);
                addException("PERCENT ", locate(ctx));
            }
        }
        sql.append(Common.FROM);
        sql.append(visitDelete_statement_from(ctx.delete_statement_from()));

        if (null != ctx.insert_with_table_hints()) {
            visitInsert_with_table_hints(ctx.insert_with_table_hints());
        }
        if (null != ctx.output_clause()) {
            visitOutput_clause(ctx.output_clause());
        }

        if (null != ctx.table_sources()) {
            // addException(" multi table deletion", locate(ctx));
            sql.append(Common.SPACE);
            sql.append(" from ");
            sql.append(visitTable_sources(ctx.table_sources()));
        }
        if (null != ctx.WHERE()) {
            sql.append(Common.SPACE).append(ctx.WHERE().getText()).append(Common.SPACE);
            if (null != ctx.search_condition()) {
                LogicNode logicNode = visitSearch_condition(ctx.search_condition());
                popStatement();
                if (null != logicNode) {
                    sql.append(logicNode.toString()).append(Common.SPACE);
                }
            }
        }
        if (null != ctx.CURRENT()) {
            //TODO 在delstatment上挂游标statement
            //CursorStatement cursorStatement = new CursorStatement();
            //addNode(cursorStatement);
            addException(" cursor", locate(ctx));
        }
        if (null != ctx.for_clause()) {
            deleteStatement.setForClause(visitFor_clause(ctx.for_clause()));
        }
        if (null != ctx.option_clause()) {
            visitOption_clause(ctx.option_clause());
        }
        cleanTableVariable();
        pushStatement(deleteStatement);
        deleteStatement.setSql(sql.toString());
        deleteStatement.addTableNames(tableNameList);
        deleteStatement.addVariables(localIdVariable);
        return deleteStatement;
    }

    /**
     * DELETE FROM Sales.SalesPersonQuotaHistory
     FROM Sales.SalesPersonQuotaHistory AS spqh
     INNER JOIN Sales.SalesPerson AS sp
     ON spqh.BusinessEntityID = sp.BusinessEntityID
     WHERE sp.SalesYTD > 2500000.00;  多表删除
     */

    /**
     * delete_statement
     * : with_expression?
     * DELETE (TOP '(' expression ')' PERCENT?)?
     * FROM? delete_statement_from
     * insert_with_table_hints?
     * output_clause?
     * (FROM table_sources)?
     * (WHERE (search_condition | CURRENT OF (GLOBAL? cursor_name | cursor_var=LOCAL_ID)))?
     * for_clause? option_clause? ';'?
     * ;
     *
     * @param ctx
     * @return
     */

    @Override
    public String visitDelete_statement_from(TSqlParser.Delete_statement_fromContext ctx) {
        StringBuffer sb = new StringBuffer();
        if (null != ctx.table_alias()) {
            sb.append(visitTable_alias(ctx.table_alias()));
        }
        if (null != ctx.ddl_object()) {
            sb.append(visitDdl_object(ctx.ddl_object()));
        }
        if (null != ctx.rowset_function_limited()) {
            visitRowset_function_limited(ctx.rowset_function_limited());
        }
        if (null != ctx.table_var) {
            String tableVar = ctx.LOCAL_ID().getText();
            sb.append(tableVar).append(Common.SPACE);
            tableNameList.add(tableVar);
            sb.append(tableVar).append(Common.SPACE);
        }
        return sb.toString();
    }

    @Override
    public String visitDdl_object(TSqlParser.Ddl_objectContext ctx) {
        StringBuffer sb = new StringBuffer();
        if (null != ctx.full_table_name()) {
            String tbName = visitFull_table_name(ctx.full_table_name()).getRealFullFuncName();
            sb.append(tbName);
            tableNameList.add(tbName);
        }
        if (null != ctx.LOCAL_ID()) {
            //local_id是一个表变量
            String tableVar = ctx.LOCAL_ID().getText();
            sb.append(tableVar).append(Common.SPACE);
            tableNameList.add(tableVar);
        }
        return sb.toString();

    }

    @Override
    public FuncName visitFull_table_name(TSqlParser.Full_table_nameContext ctx) {
        String server = "";
        String database = "";
        String funcName = "";
        String schema = "";
        if (ctx.server != null) {
            server = ctx.server.getText();
        }
        if (ctx.database != null) {
            database = ctx.database.getText();
        }
        if (ctx.schema != null) {
            schema = ctx.schema.getText();
        }
        funcName = ctx.table.getText();
        FuncName fn = new FuncName(server, database, funcName, schema);
        return fn;
    }


    @Override
    public String visitInsert_with_table_hints(TSqlParser.Insert_with_table_hintsContext ctx) {
     /*   StringBuffer rs = new StringBuffer();
        rs.append(ctx.WITH().getText());
        rs.append("(");
        for (int i = 0; i < ctx.table_hint().size(); i++) {
            if (i != 0) {
                rs.append(",").append(Common.SPACE);
            }
            rs.append(visitTable_hint(ctx.table_hint(i)));
        }
        rs.append(")");*/

        if (ctx.table_hint().size() > 0) {
            visitTable_hint(ctx.table_hint(0));
        }
        return "";
    }

    /**
     * 返回受 INSERT、UPDATE、DELETE 或 MERGE 语句影响的各行中的信息，
     * 或返回基于受这些语句影响的各行的表达式。 这些结果可以返回到处理应用程序，以供在确认消息、存档以及其他类似的应用程序要求中使用。
     * 也可以将这些结果插入表或表变量。
     * 另外，您可以捕获嵌入的 INSERT、UPDATE、DELETE 或 MERGE 语句中 OUTPUT 子句的结果，
     * 然后将这些结果插入目标表或视图
     * <p>
     * output_clause
     * : OUTPUT output_dml_list_elem (',' output_dml_list_elem)*
     * (INTO (LOCAL_ID | table_name) ('(' column_name_list ')')? )?
     * ;
     *
     * @param ctx
     * @return
     */
    @Override
    public String visitOutput_clause(TSqlParser.Output_clauseContext ctx) {
        //SqlStatement rs = new SqlStatement();
        if (null != ctx.OUTPUT()) {
            addException("output  option", locate(ctx));
        }
        return "";
    }

    //=============================select_statement====================

    /**
     * (1)select 语句 一个特殊情况
     *
     * DECLARE @aa VARCHAR(200)
     * set @aa='dddd'
     * print @aa
     * select  @aa=string2  from boolean_ana where string1 ='code'
     * PRINT @aa
     *
     * ===》执行步骤： 当select 中有值时,需要在select 语句执行完,执行结果为rs,在执行set @aa = (rs中最后一条结果)
     *
     *
     * (2)变量中替换
     *
     * DECLARE @cond INT
     * SET @cond = 1
     * SELECT	COUNT (1) AS cnt FROM HumanResources.Employee
     * WHERE OrganizationLevel > @cond AND OrganizationLevel < @cond
     *
     *===>执行步骤 ： （1）visit的时候表达式中如果遇到LOCAL_ID即@cond这样的，存入一个set
     *                 (2)执行的时候去替换@cond
     *
     *
     *
     *
     *
     *
     */

    /**
     * 保存sql中的变量名字,对应于g4文件中的localId
     * 如 WHERE OrganizationLevel > @cond AND OrganizationLevel < @cond sql中的@cond
     */
    private Set<String> localIdVariable = new HashSet<String>();

    /**
     * 将执行sql的结果 赋于一个变量
     * 如select  @aa=string2  from boolean_ana where string1 ='code'
     */
    private List<String> resultSetVariable = new ArrayList<>();


    private Set<String> tableNameList = new HashSet<>();


    private String limitSql = "";

    private void clearVariable() {
        limitSql = "";
    }

    @Override
    public SqlStatement visitSelect_statement(TSqlParser.Select_statementContext ctx) {
        SelectStatement selectStatement = new SelectStatement(Common.SELECT);
        StringBuffer sql = new StringBuffer();
        clearVariable();
        boolean withExpression = false;
        String fromTableName = "";
        String clusterByColumnName = "";
        if (null != ctx.with_expression()) {
            sql.append(visitWith_expression(ctx.with_expression()).getSql());
            withExpression = true;
            clusterByColumnName = getOutPutColumnFromWithExpress(ctx.with_expression());
            if (StringUtils.isBlank(clusterByColumnName)) {
                fromTableName = getTableNameFromWitheExpression(ctx.with_expression());
            }
        }
        QueryExpressionBean queryExpressionBean = visitQuery_expression(ctx.query_expression());
        if (null != queryExpressionBean.getQuerySpecificationBean()) {
            SelectIntoBean selectIntoBean = queryExpressionBean.getQuerySpecificationBean().getSelectIntoBean();
            if (withExpression && null != selectIntoBean) {
                if (StringUtils.isNotBlank(clusterByColumnName)) {
                    selectIntoBean.setClusterByColumnName(clusterByColumnName);
                } else {
                    selectIntoBean.setSourceTableName(fromTableName);
                }
            }
            selectStatement.setSelectIntoBean(selectIntoBean);
        }
        sql.append(queryExpressionBean.getSql());
        if (null != queryExpressionBean.getExceptions() && !queryExpressionBean.getExceptions().isEmpty()) {
            for (String s : queryExpressionBean.getExceptions()) {
                addException(s, locate(ctx));
            }
        }
        if (null != ctx.order_by_clause()) {
            sql.append(visitOrder_by_clause(ctx.order_by_clause()));
        }

        if (null != ctx.option_clause()) {
            sql.append(visitOrder_by_clause(ctx.order_by_clause()));
        }
        SelectIntoBean finalSelectIntoBean = selectStatement.getSelectIntoBean();
        if (null != finalSelectIntoBean) {
            TSqlParser.Query_specificationContext query = ctx.query_expression().query_specification();
            if(StringUtils.isBlank(finalSelectIntoBean.getClusterByColumnName())){
                finalSelectIntoBean.setClusterByColumnName(getOutPutColumnFromQuery(query));
            }
            finalSelectIntoBean.setSourceTableName(getTbNameFromTableSourceContext(query.table_sources()));
        }
        // with tmpShip as (select * from t11) select * into #tmp from tmpShip;
        // 由于spark不支持with tmpShip as (  select * from t1   ) create table tmp.tmp_tmp_1494555472055_691 as select * from tmpShip
        // spark 支持with tmpShip as (  select * from t1   ) select * from tmpShip
        // 需要修改为 create table tmp.tmp_tmp_1494555472055_691 as select * from  (  select * from t1   ) tmpShip

        if (null != finalSelectIntoBean && withExpression) {
            String withCreateTableSql = queryExpressionBean.getQuerySpecificationBean().getSql();
            Set<String> withKeys = withExpressionSqlMap.keySet();
            Iterator<String> withkeyIt = withKeys.iterator();
            while (withkeyIt.hasNext()) {
                String key = withkeyIt.next();
                String replaceSql = "  (" + withExpressionSqlMap.get(key) + ") " + key + " ";
                withCreateTableSql = withCreateTableSql.replaceAll(" " + key, replaceSql);
            }
            sql.setLength(0);
            sql.append(withCreateTableSql);
        }

        sql.append(limitSql);
        selectStatement.setSql(sql.toString());
        selectStatement.addVariables(localIdVariable);
        selectStatement.addResultSetVariables(resultSetVariable);
        selectStatement.addTableNames(tableNameList);


        clearVariable();

        if (null != ctx.for_clause()) {
            selectStatement.setForClause(visitFor_clause(ctx.for_clause()));
        }

        addNode(selectStatement);
        pushStatement(selectStatement);
        return selectStatement;
    }


    @Override
    public ForClause visitFor_clause(TSqlParser.For_clauseContext ctx) {
        //SqlStatement rs = new SqlStatement();
//        addException(" for clause ", locate(ctx));
        ForClause forClause = new ForClause();
        if (null != ctx.XML()) {
            forClause.setXformat(ForClause.XFORMAT.XML);
        }
        if (null != ctx.AUTO()) {
            forClause.setXmlMode(ForClause.XMLMODE.AUTO);
        }
        if (null != ctx.xml_common_directives()) {
            forClause.setDirectives(visitXml_common_directives(ctx.xml_common_directives()));
        }

        if (null != ctx.STRING()) {
            forClause.setRow(StrUtils.trimQuot(ctx.STRING().getText()));
        }

        return forClause;
    }


    @Override
    public ForClause.DIRECTIVES visitXml_common_directives(TSqlParser.Xml_common_directivesContext ctx) {
        ForClause.DIRECTIVES directives = ForClause.DIRECTIVES.BINARY;
        if (null != ctx.ROOT()) {
            directives = ForClause.DIRECTIVES.ROOT;
        }
        if (null != ctx.TYPE()) {
            directives = ForClause.DIRECTIVES.TYPE;
        }
        return directives;

    }

    @Override
    public String visitOption_clause(TSqlParser.Option_clauseContext ctx) {
        //SqlStatement rs = new SqlStatement();
        //如指定查询中的 JOIN 操作由 MERGE JOIN 执行，可以使用 MAXRECURSION 来防止不合理的递归公用表表达式进入无限循环等操作
        addException("query option", locate(ctx));
        return "";
    }

    @Override
    public WithExpressionBean visitWith_expression(TSqlParser.With_expressionContext ctx) {
        WithExpressionBean withExpressionBean = new WithExpressionBean();
        List<CommonTableExpressionBean> list = new ArrayList<>();
        if (null != ctx.XMLNAMESPACES()) {
            addException(ctx.XMLNAMESPACES().toString(), locate(ctx));
        }
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append("with ");
        if (!ctx.common_table_expression().isEmpty()) {
            for (int i = 0; i < ctx.common_table_expression().size(); i++) {
                if (i != 0) {
                    stringBuffer.append(",").append(Common.SPACE);
                }
                CommonTableExpressionBean commonTableExpressionBean = visitCommon_table_expression(ctx.common_table_expression(i));
                list.add(commonTableExpressionBean);
                stringBuffer.append(commonTableExpressionBean.getSql());
            }
        }
        withExpressionBean.setSql(stringBuffer.toString());
        withExpressionBean.setList(list);
        return withExpressionBean;
    }

    private Queue<String> withColmnNameAlias = new LinkedList<>();

    /**
     * common_table_expression
     * : expression_name=id ('(' column_name_list ')')? AS '(' select_statement ')'
     *
     * @param ctx
     * @return
     */

    @Override
    public CommonTableExpressionBean visitCommon_table_expression(TSqlParser.Common_table_expressionContext ctx) {
        CommonTableExpressionBean commonTableExpressionBean = new CommonTableExpressionBean();
        StringBuffer stringBuffer = new StringBuffer();
        withColmnNameAlias.clear();
        if (null != ctx.column_name_list()) {
            List<String> list = visitColumn_name_list(ctx.column_name_list());
            commonTableExpressionBean.setColumnNameList(list);
            for (String c : list) {
                withColmnNameAlias.add(c);
            }
        }
        String expressionName = visitId(ctx.id());
        commonTableExpressionBean.setExpressionName(expressionName);
        stringBuffer.append(expressionName).append(Common.SPACE);
        stringBuffer.append("as (").append(Common.SPACE);
        String sql = visitSelect_statement(ctx.select_statement())
                .getSql().toString().replaceAll(Common.SEMICOLON, Common.SPACE);
        commonTableExpressionBean.setSelectsql(sql);
        stringBuffer.append(sql);
        withExpressionSqlMap.put(visitId(ctx.id()), sql);
        stringBuffer.append(" )").append(Common.SPACE);
        popStatement();
        withColmnNameAlias.clear();
        commonTableExpressionBean.setSql(stringBuffer.toString());
        return commonTableExpressionBean;
    }

    //: (query_specification | '(' query_expression ')') union*
    @Override
    public QueryExpressionBean visitQuery_expression(TSqlParser.Query_expressionContext ctx) {
        StringBuffer sql = new StringBuffer();
        QueryExpressionBean queryExpressionBean = new QueryExpressionBean();
        if (null != ctx.query_expression()) {
            QueryExpressionBean leftQueryExpressionBean = visitQuery_expression(ctx.query_expression());
            queryExpressionBean.setQueryExpressionBean(leftQueryExpressionBean);
        } else if (null != ctx.query_specification()) {
            QuerySpecificationBean querySpecificationBean = visitQuery_specification(ctx.query_specification());
            queryExpressionBean.setQuerySpecificationBean(querySpecificationBean);
        }
        List<UnionBean> unionBeanList = new ArrayList<>();
        for (int j = 0; j < ctx.union().size(); j++) {
            UnionBean unionBean = visitUnion(ctx.union(j));
            unionBeanList.add(unionBean);
        }
        queryExpressionBean.setUnionBeanList(unionBeanList);
        /*if (null != ctx.query_expression()) {
            sql.append("(");
            sql.append(visitQuery_expression(ctx.query_expression()));
            sql.append(")");
        } else {
            QuerySpecificationBean rightQueryBean = new QuerySpecificationBean();
            QuerySpecificationBean leftQueryBean = visitQuery_specification(ctx.query_specification());
            leftQueryBean.setTableName(getTableNameByUUId());
            Stack<String> unionSql = new Stack<>();
            UnionBean.UnionType unionType = null;
            boolean popflag = false;
            for (int j = ctx.union().size() - 1; j >= 0; j--) {
                if (j != 0) {
                    UnionBean leftUnionBean = visitUnion(ctx.union(j - 1));
                    leftQueryBean = leftUnionBean.getQuerySpecificationBean();
                    leftQueryBean.setTableName(getTableNameByUUId());
                } else {
                    leftQueryBean = visitQuery_specification(ctx.query_specification());
                    leftQueryBean.setTableName(getTableNameByUUId());
                }
                UnionBean unionBean = visitUnion(ctx.union(j));
                if (null != unionType && !unionType.equals(unionBean.getUnionType())) {
                    addException("UNION ,EXCEPT, INTERSECT  ", locate(ctx));
                }
                unionType = unionBean.getUnionType();
                if (j == ctx.union().size() - 1 || !popflag) {
                    rightQueryBean = unionBean.getQuerySpecificationBean();
                    rightQueryBean.setTableName(getTableNameByUUId());
                }
                if (unionType.equals(UnionBean.UnionType.EXCEPT) || unionType.equals(UnionBean.UnionType.INTERSECT)
                        && null != rightQueryBean) {
                    String s = createExceptOrIntersectSql(leftQueryBean, rightQueryBean, unionType);
                    if (popflag) {
                        unionSql.pop();
                    }
                    rightQueryBean.setSql(s);
                    rightQueryBean.setTableName(getTableNameByUUId());
                    rightQueryBean.setSelectList(leftQueryBean.getSelectList());
                    unionSql.push(s);
                    popflag = true;
                } else {
                    if (popflag) {
                        unionSql.push(" union all " + unionSql.pop());
                    } else {
                        unionSql.push(unionBean.getSql());
                    }
                    popflag = false;
                }
            }
            if (!popflag) {
                unionSql.push(leftQueryBean.getSql());
            }
            while (!unionSql.empty()) {
                sql.append(Common.SPACE).append(unionSql.pop()).append(Common.SPACE);
            }

        }
        return sql.toString();*/
        return queryExpressionBean;
    }

    private String getTableNameByUUId() {
        String t = "except_";
        return t + UUID.randomUUID().toString().substring(15).replaceAll("-", "_");

    }

    private String getOutPutColumnFromQuery(TSqlParser.Query_specificationContext ctx) {
        ArrayList<ColumnBean> columns = visitSelect_list(ctx.select_list());
        String columnName = "";
        if (null != columns && !columns.isEmpty()) {
            ColumnBean columnBean = columns.get(0);
            if (null != columnBean && StringUtils.isNotBlank(columnBean.getRealColumnName())) {
                columnName = columnBean.getRealColumnName();
            } else {
                columnName = getOutPutColumnFromTable(ctx.table_sources());
            }
        }
        return columnName;
    }

    private String getOutPutColumnFromWithExpress(TSqlParser.With_expressionContext ctx) {
        String columnName = "";
        if (null != ctx.common_table_expression(0)) {
            TSqlParser.Common_table_expressionContext comTbEx = ctx.common_table_expression(0);
            if (null != comTbEx.column_name_list()) {
                columnName = visitColumn_name_list(comTbEx.column_name_list()).get(0);
                return columnName;
            }
            TSqlParser.Query_specificationContext querySp = comTbEx.select_statement().
                    query_expression().query_specification();
            columnName = getOutPutColumnFromQuery(querySp);
        }
        return columnName;
    }

    private String getTableNameFromWitheExpression(TSqlParser.With_expressionContext ctx) {
        String tableName = "";
        if (null != ctx.common_table_expression(0)) {
            TSqlParser.Common_table_expressionContext comTbEx = ctx.common_table_expression(0);
            TSqlParser.Query_specificationContext querySp = comTbEx.select_statement().
                    query_expression().query_specification();
            tableName = getTbNameFromTableSourceContext(querySp.table_sources());
        }
        return tableName;
    }

    private String getOutPutColumnFromTable(TSqlParser.Table_sourcesContext ctx) {
        List<TSqlParser.Table_sourceContext> tbSources = ctx.table_source();
        String columnName = "";
        if (null != tbSources.get(0).table_source_item_joined().table_source_item().derived_table()) {
            TSqlParser.Derived_tableContext derivedTable = tbSources.get(0).table_source_item_joined()
                    .table_source_item().derived_table();
            TSqlParser.Query_specificationContext qs = derivedTable.subquery().
                    select_statement().query_expression().query_expression().query_specification();
            if (null != qs) {
                columnName = getOutPutColumnFromQuery(qs);
            }
        }
        return columnName;
    }

    private String getTbNameFromTableSourceContext(TSqlParser.Table_sourcesContext ctx) {
        List<TSqlParser.Table_sourceContext> tbSources = ctx.table_source();
        String tableName = "";
        TSqlParser.Table_name_with_hintContext tableNameWithHint = tbSources.get(0).table_source_item_joined()
                .table_source_item().table_name_with_hint();
        if (null != tableNameWithHint) {
            tableName = visitTable_name(tableNameWithHint.table_name()).getRealFullFuncName();
        } else {
            TSqlParser.Derived_tableContext derivedTable = tbSources.get(0).table_source_item_joined()
                    .table_source_item().derived_table();
            if (null != derivedTable) {
                TSqlParser.Table_sourcesContext tableSources = derivedTable.subquery().select_statement()
                        .query_expression().query_expression().query_specification().table_sources();
                tableName = getTbNameFromTableSourceContext(tableSources);
            }
        }

        return tableName;
    }


    @Override
    public QuerySpecificationBean visitQuery_specification(TSqlParser.Query_specificationContext ctx) {
        QuerySpecificationBean querySpecificationBean = new QuerySpecificationBean();
        String intoTableSql = " ";
        StringBuffer rs = new StringBuffer();
        rs.append(ctx.SELECT().getText()).append(Common.SPACE);
        if (null != ctx.ALL()) {
            rs.append(ctx.ALL().getText()).append(Common.SPACE);
        }
        if (null != ctx.DISTINCT()) {
            rs.append(ctx.DISTINCT().getText()).append(Common.SPACE);
        }
        ArrayList<ColumnBean> columns = visitSelect_list(ctx.select_list());
        querySpecificationBean.setSelectList(columns);
        rs.append(StringUtils.join(columns.toArray(), ","));
        SelectIntoBean selectIntoBean = new SelectIntoBean();
        if (null != ctx.INTO()) {
            String tableName = visitTable_name(ctx.table_name()).getRealFullFuncName();
            tableNameList.add(tableName);
            selectIntoBean.setIntoTableName(tableName);
            querySpecificationBean.setSelectIntoBean(selectIntoBean);
            if (procFlag && !columns.isEmpty() && null != columns.get(0)) {
                String clusterByColumnName = columns.get(0).getRealColumnName();
                selectIntoBean.setClusterByColumnName(clusterByColumnName);
                if (StringUtils.isNotBlank(clusterByColumnName)) {
                    intoTableSql = "create table " + tableName + String.format(Common.crudStr, StrUtils.addBackQuote(clusterByColumnName)) + " as ";
                } else {
                    intoTableSql = "create table " + tableName + Common.SPACE + Common.crudStr + " as ";
                }
            } else {
                intoTableSql = "create table " + tableName + " as ";
            }
        }
        if (null != ctx.FROM()) {
            rs.append(ctx.FROM().getText()).append(Common.SPACE);
            String fromTb = visitTable_sources(ctx.table_sources());
            selectIntoBean.setSourceTableName(getTbNameFromTableSourceContext(ctx.table_sources()));
            rs.append(fromTb).append(Common.SPACE);
        }
        if (null != ctx.WHERE()) {
            rs.append(ctx.WHERE().getText()).append(Common.SPACE);
            rs.append(visitSearch_condition(ctx.search_condition(0)).toString()).append(Common.SPACE);
            popStatement();
        }
        if (null != ctx.GROUP()) {
            rs.append(ctx.GROUP().getText()).append(Common.SPACE);
            rs.append(ctx.BY().getText()).append(Common.SPACE);
            if (!ctx.group_by_item().isEmpty()) {
                for (int i = 0; i < ctx.group_by_item().size(); i++) {
                    if (i != 0) {
                        rs.append(",").append(Common.SPACE);
                    }
                    rs.append(visitGroup_by_item(ctx.group_by_item(i)));
                }
            }
        }
        if (null != ctx.HAVING()) {
            rs.append(ctx.HAVING().getText()).append(Common.SPACE);
            if (ctx.search_condition().size() == 2) {
                LogicNode logicNode = visitSearch_condition(ctx.search_condition(1));
                if (null != logicNode) {
                    rs.append(logicNode.toString()).append(Common.SPACE);
                    popStatement();
                }
            } else {
                LogicNode logicNode = visitSearch_condition(ctx.search_condition(0));
                if (null != logicNode) {
                    rs.append(logicNode.toString()).append(Common.SPACE);
                    popStatement();
                }
            }
        }
        if (null != ctx.TOP()) {
            StringBuffer sql = new StringBuffer().append(Common.LIMIT).append(Common.SPACE);
            TreeNode sqlStatement = (TreeNode) visit(ctx.expression());
            popStatement();
            sql.append(sqlStatement.getSql()).append(Common.SPACE);
            if (null != ctx.PERCENT()) {
                //rs.sql.append(ctx.PERCENT().getText()).append(Common.SPACE);
                addException("PERCENT", locate(ctx));
            }
            limitSql = sql.toString();
        }
        querySpecificationBean.setSql(intoTableSql + rs.toString());
        return querySpecificationBean;
    }

    @Override
    public String visitGroup_by_item(TSqlParser.Group_by_itemContext ctx) {
        TreeNode ss = (TreeNode) visit(ctx.expression());
        popStatement();
        return ss.getSql();
    }

    @Override
    public String visitTable_sources(TSqlParser.Table_sourcesContext ctx) {
        StringBuffer rs = new StringBuffer();
        if (!ctx.table_source().isEmpty()) {
            for (int i = 0; i < ctx.table_source().size(); i++) {
                if (i != 0) {
                    rs.append(",").append(Common.SPACE);
                }
                rs.append(visitTable_source(ctx.table_source(i))).append(Common.SPACE);
            }
        }
        return rs.toString();
    }

    @Override
    public String visitTable_source(TSqlParser.Table_sourceContext ctx) {
        StringBuffer rs = new StringBuffer();
        if (ctx.getText().contains("(")) {
            rs.append("(").append(Common.SPACE);
        }
        if (null != ctx.table_source_item_joined()) {
            rs.append(visitTable_source_item_joined(ctx.table_source_item_joined()));
        }
        if (ctx.getText().contains(")")) {
            rs.append(")").append(Common.SPACE);
        }
        return rs.toString();
    }

    @Override
    public String visitTable_source_item_joined(TSqlParser.Table_source_item_joinedContext ctx) {
        StringBuffer rs = new StringBuffer();
        rs.append(visitTable_source_item(ctx.table_source_item()));
        for (int i = 0; i < ctx.join_part().size(); i++) {
            rs.append(visitJoin_part(ctx.join_part(i)));
        }
        return rs.toString();
    }


    @Override
    public String visitTable_source_item(TSqlParser.Table_source_itemContext ctx) {
        StringBuffer rs = new StringBuffer();
        if (null != ctx.column_alias_list()) {
            List<String> list = visitColumn_alias_list(ctx.column_alias_list());
            for (String c : list) {
                withColmnNameAlias.add(c);
            }
        }
        if (null != ctx.table_name_with_hint()) {
            rs.append(visitTable_name_with_hint(ctx.table_name_with_hint()));
            if (null != ctx.as_table_alias()) {
                rs.append(visitAs_table_alias(ctx.as_table_alias()));
            }
        }
        if (null != ctx.rowset_function()) {
            visitRowset_function(ctx.rowset_function());
        }
        if (null != ctx.derived_table()) {
            rs.append(visitDerived_table(ctx.derived_table())).append(Common.SPACE);
            if (null != ctx.as_table_alias()) {
                rs.append(visitAs_table_alias(ctx.as_table_alias()));
            }
        }
        if (null != ctx.change_table()) {
            visitChange_table(ctx.change_table());
        }
        if (null != ctx.function_call()) {
            //rs.append(visitFunction_call_expression(ctx.function_call()));
            if (null != ctx.as_table_alias()) {
                rs.append(visitAs_table_alias(ctx.as_table_alias()));
            }
        }
        if (null != ctx.LOCAL_ID()) {
            if (null != ctx.as_table_alias()) {
                tableNameList.add(ctx.LOCAL_ID().getText());
                rs.append(ctx.LOCAL_ID().getText()).append(".");
                if (null != ctx.function_call()) {
//                    rs.append(visitFunction_call(ctx.function_call()));
                }
                if (null != ctx.as_table_alias()) {
                    rs.append(visitAs_table_alias(ctx.as_table_alias()));
                    if (null != ctx.column_alias_list()) {
                        rs.append(StrUtils.concat(visitColumn_alias_list(ctx.column_alias_list())));
                    }
                }
            } else {
                rs.append(ctx.LOCAL_ID().getText()).append(Common.SPACE);
                tableNameList.add(ctx.LOCAL_ID().getText());
                if (null != ctx.as_table_alias()) {
                    rs.append(visitAs_table_alias(ctx.as_table_alias()));
                }
            }
        }

        return rs.toString();
    }


    @Override
    public List<String> visitColumn_alias_list(TSqlParser.Column_alias_listContext ctx) {
        List<String> columnAliasList = new ArrayList<>();

        StringBuffer rs = new StringBuffer();
        if (!ctx.column_alias().isEmpty()) {
            for (int i = 0; i < ctx.column_alias().size(); i++) {
               /* if (i != 0) {
                    rs.append(",").append(Common.SPACE);
                }
                rs.append(visitColumn_alias(ctx.column_alias(i)));*/
                columnAliasList.add(visitColumn_alias(ctx.column_alias(i)));
            }
        }
        return columnAliasList;
    }

    @Override
    public String visitColumn_alias(TSqlParser.Column_aliasContext ctx) {
        //如果列名中含有空格和单引号，sparksql不支持，如 select name AS 'Time Range' from tb
        //需要转化为`Time Range`
        String columnAliasName = "";
        if (null != ctx.id()) {
            columnAliasName = visitId(ctx.id());
        } else {
            columnAliasName = ctx.getText().trim();
        }
        StringBuffer stringBuffer = new StringBuffer();
        if (columnAliasName.indexOf("'") != -1 || columnAliasName.lastIndexOf("'") != -1) {
            stringBuffer.append("`").append(columnAliasName.substring(1, columnAliasName.length() - 1));
            stringBuffer.append("`");
            return stringBuffer.toString();
        } else if (columnAliasName.indexOf("\"") != -1 && columnAliasName.lastIndexOf("\"") != -1) {
            stringBuffer.append("`").append(columnAliasName.substring(1, columnAliasName.length() - 1));
            stringBuffer.append("`");
            return stringBuffer.toString();
        }
        return stringBuffer.append("`").append(columnAliasName).append("`").toString();
    }

    @Override
    public SqlStatement visitAggregate_windowed_function(TSqlParser.Aggregate_windowed_functionContext ctx) {
        SqlStatement rs = new SqlStatement();
        StringBuffer sql = new StringBuffer();
        if (null != ctx.GROUPING_ID()) {
            sql.append(ctx.GROUPING_ID().getText()).append("(");
            TreeNode exprs = visitExpression_list(ctx.expression_list());
            sql.append(exprs.getSql());
            sql.append(")");
        } else if (null != ctx.GROUPING()) {
            sql.append(ctx.GROUPING().getText()).append("(");
            sql.append(getExpressionSql(ctx.expression()));
            popStatement();
            sql.append(")");
        } else if (null != ctx.CHECKSUM_AGG()) {
            sql.append(ctx.CHECKSUM_AGG().getText()).append("(");
            sql.append(visitAll_distinct_expression(ctx.all_distinct_expression()));
            sql.append(")");
        } else if (null != ctx.COUNT() || null != ctx.COUNT_BIG()) {
            sql.append(" count");
            sql.append("(");
            if (null != ctx.all_distinct_expression()) {
                sql.append(visitAll_distinct_expression(ctx.all_distinct_expression()));

            } else {
                sql.append("*");
            }
            sql.append(")");
        } else {
            if (null != ctx.AVG()) {
                sql.append(ctx.AVG().getText());
            }
            if (null != ctx.MAX()) {
                sql.append(ctx.MAX().getText());
            }
            if (null != ctx.MIN()) {
                sql.append(ctx.MIN().getText());
            }
            if (null != ctx.SUM()) {
                sql.append(ctx.SUM().getText());
            }
            if (null != ctx.STDEV()) {
                sql.append(ctx.STDEV().getText());
            }
            if (null != ctx.STDEVP()) {
                sql.append(ctx.STDEVP().getText());
            }
            if (null != ctx.VAR()) {
                sql.append(ctx.VAR().getText());
            }
            if (null != ctx.VARP()) {
                sql.append(ctx.VARP().getText());
            }
            sql.append("(");
            sql.append(visitAll_distinct_expression(ctx.all_distinct_expression()));
            sql.append(")");
            if (null != ctx.over_clause()) {
                sql.append(visitOver_clause(ctx.over_clause()));
            }
        }
        rs.setSql(sql.append(Common.SPACE).toString());
        pushStatement(rs);
        return rs;
    }

    @Override
    public String visitAll_distinct_expression(TSqlParser.All_distinct_expressionContext ctx) {
        StringBuffer rs = new StringBuffer();
        if (null != ctx.ALL()) {
            rs.append(ctx.ALL().getText()).append(Common.SPACE);
        }
        if (null != ctx.DISTINCT()) {
            rs.append(ctx.DISTINCT().getText()).append(Common.SPACE);
        }
        rs.append(getExpressionSql(ctx.expression()));
        popStatement();
        return rs.toString();
    }

    @Override
    public FuncName visitScalar_function_name(TSqlParser.Scalar_function_nameContext ctx) {
        if (null != ctx.func_proc_name()) {
            return visitFunc_proc_name(ctx.func_proc_name());
        }
        if (null != ctx.RIGHT()) {
            return new FuncName(null, ctx.RIGHT().getText(), null);
        }
        if (null != ctx.BINARY_CHECKSUM()) {
            return new FuncName(null, ctx.BINARY_CHECKSUM().getText(), null);
        }
        if (null != ctx.LEFT()) {
            return new FuncName(null, ctx.LEFT().getText(), null);
        }
        if (null != ctx.CHECKSUM()) {
            return new FuncName(null, ctx.CHECKSUM().getText(), null);
        }
        return null;
    }

    @Override
    public TreeNode visitLog_function(TSqlParser.Log_functionContext ctx) {
        LogFunction function = new LogFunction(new FuncName(null, "LOG", null));
        visit(ctx.expression());
        function.setExpr(popStatement());
        pushStatement(function);
        return function;
    }

//    @Override
//    public Object visitRanking_win_function(TSqlParser.Ranking_win_functionContext ctx) {
//        return super.visitRanking_win_function(ctx);
//    }

    @Override
    public Object visitAggregate_win_function(TSqlParser.Aggregate_win_functionContext ctx) {
        return super.visitAggregate_win_function(ctx);
    }


    @Override
    public TreeNode visitTsubstring_function(TSqlParser.Tsubstring_functionContext ctx) {
//        return super.visitTsubstring_function(ctx);
        TSubstringFunction func = new TSubstringFunction(new FuncName(null, "SUBSTRING", null));
        List<TSqlParser.ExpressionContext> list = ctx.expression();
        visit(list.get(0));
        func.setLeftExpr(popStatement());
        visit(list.get(1));
        func.setMidExpr(popStatement());
        visit(list.get(2));
        func.setRightExpr(popStatement());
        pushStatement(func);
        return func;
    }


    @Override
    public TreeNode visitLen_function(TSqlParser.Len_functionContext ctx) {
        LenFunction function = new LenFunction(new FuncName(null, "LEN", null));
        visit(ctx.expression());
        function.setExpr(popStatement());
        pushStatement(function);
        return function;

    }

    @Override
    public BaseFunction visitScalar_function(TSqlParser.Scalar_functionContext ctx) {
        FuncName funcName = visitScalar_function_name(ctx.scalar_function_name());
        ScalarFunction function = new ScalarFunction(funcName);
        if (null != ctx.expression_list()) {
            visitExpression_list(ctx.expression_list());
            function.setExprs(popStatement());
        }

        pushStatement(function);
        return function;
    }

    @Override
    public Object visitBinary_checksum_function(TSqlParser.Binary_checksum_functionContext ctx) {
        return super.visitBinary_checksum_function(ctx);
    }

    @Override
    public BaseFunction visitCast_function(TSqlParser.Cast_functionContext ctx) {
//        return super.visitCast_function(ctx);
        CastFunction function = new CastFunction(new FuncName(null, "CAST", null));
        visit(ctx.expression());
        function.setExpr(popStatement());
        function.setDataType(visitData_type(ctx.data_type()));
        if (ctx.data_type().getChildCount() == 4) {
            function.setLength(Integer.valueOf(ctx.data_type().getChild(2).getText().trim()));
        }
        pushStatement(function);
        return function;
    }

    @Override
    public BaseFunction visitCast_and_add(TSqlParser.Cast_and_addContext ctx) {
        visit(ctx.function_call());
        TreeNode expr = popStatement();
        int incr = Integer.parseInt(ctx.DECIMAL().getText());
        String unitStr = null;
        if (null != ctx.DAYS()) {
            unitStr = ctx.DAYS().getText();
        }
        if (null != ctx.YEARS()) {
            unitStr = ctx.YEARS().getText();
        }
        if (null != ctx.MINUTES()) {
            unitStr = ctx.MINUTES().getText();
        }
        if (null != ctx.HOURS()) {
            unitStr = ctx.HOURS().getText();
        }

        TimeUnit unit = TimeUnit.DAYS;

        if (StringUtils.isNotBlank(unitStr)) {
            unit = TimeUnit.valueOf(unitStr.toUpperCase());
        }
        if (unit != TimeUnit.DAYS) {
            addException("Only support DAYS", locate(ctx));
        }


        CastAndAddFunction function = new CastAndAddFunction(new FuncName(null, "CastAndAdd", null));
        function.setExpr(expr);
        function.setIncr(incr);
        function.setTimeUnit(unit);
        function.setOperate(ctx.sign().getText().trim());
        pushStatement(function);
        return function;
    }

    @Override
    public BaseFunction visitConvert_function(TSqlParser.Convert_functionContext ctx) {
        ConvertFunction function = new ConvertFunction(new FuncName(null, "CONVERT", null));
        visit(ctx.expression().get(0));
        function.setExpr(popStatement());
        function.setDataType(visitData_type(ctx.data_type()));
        if (ctx.data_type().getChildCount() == 4) {
            function.setLength(Integer.valueOf(ctx.data_type().getChild(2).getText().trim()));
        }
        if (ctx.expression().size() == 2) {
            visit(ctx.expression().get(1));
            function.setStyle(popStatement());
        }

        pushStatement(function);
        return function;
    }

    @Override
    public Object visitChecksum_function(TSqlParser.Checksum_functionContext ctx) {
        return super.visitChecksum_function(ctx);
    }

    @Override
    public BaseFunction visitCoalesce_function(TSqlParser.Coalesce_functionContext ctx) {
        CoalesceFunction function = new CoalesceFunction(new FuncName(null, "coalesce", null));
        if (null != ctx.expression_list()) {
            visitExpression_list(ctx.expression_list());
            function.setExprs(popStatement());
        }
        pushStatement(function);
        return function;
    }

    @Override
    public BaseFunction visitCurrent_timestamp_function(TSqlParser.Current_timestamp_functionContext ctx) {
//        return super.visitCurrent_timestamp_function(ctx);
        FuncName funcName = new FuncName(null, "GETDATE", null);
        ScalarFunction function = new ScalarFunction(funcName);
        pushStatement(function);
        return function;
    }

    @Override
    public Object visitCurrent_user_function(TSqlParser.Current_user_functionContext ctx) {
        return super.visitCurrent_user_function(ctx);
    }

    @Override
    public BaseFunction visitDateadd_function(TSqlParser.Dateadd_functionContext ctx) {
//        return super.visitDateadd_function(ctx);
        DateAddFunction function = new DateAddFunction(new FuncName(null, "DATEADD", null));
        String datePart = ctx.ID().getText();
        DateUnit unit = DateUnit.parse(datePart);
        if (null == unit) {
            this.addException("datepart #[" + datePart + "]", locate(ctx));
        }
        function.setDatePart(unit);
        visit(ctx.expression().get(0));
        function.setNumber(popStatement());
        visit(ctx.expression().get(1));
        function.setDate(popStatement());
        pushStatement(function);
        return function;
    }

    @Override
    public BaseFunction visitDatediff_function(TSqlParser.Datediff_functionContext ctx) {
        DatediffFunction function = new DatediffFunction(new FuncName(null, "DATEDIFF", null));
        String datePart = ctx.ID().getText();
        DateUnit unit = DateUnit.parse(datePart);
        if (null == unit) {
            this.addException("datepart #[" + datePart + "]", locate(ctx));
        }
        function.setDatePart(unit);

        visit(ctx.expression().get(0));
        function.setLeftExpr(popStatement());
        visit(ctx.expression().get(1));
        function.setRightExpr(popStatement());
        pushStatement(function);
        return function;
    }

    @Override
    public BaseFunction visitDatename_function(TSqlParser.Datename_functionContext ctx) {
        DateNameFunction function = new DateNameFunction(new FuncName(null, "DATENAME", null));
        String datePart = ctx.ID().getText();
//        if ("weekday".equals(datePart) || "dw".equals(datePart) || "w".equals(datePart)) {
//            addException("DatePart " + datePart, locate(ctx));
//        }
        DateUnit dateUnit = DateUnit.parse(datePart);
        if (null == dateUnit) {
            addException("datepart # " + datePart, locate(ctx));
        }
        function.setDateUnit(dateUnit);
        visit(ctx.expression());
        function.setExpr(popStatement());
        pushStatement(function);
        return function;
    }

    @Override
    public BaseFunction visitDatepart_function(TSqlParser.Datepart_functionContext ctx) {
        DateNameFunction function = new DateNameFunction(new FuncName(null, "DATEPART", null));
        String datePart = ctx.ID().getText();
        DateUnit dateUnit = DateUnit.parse(datePart);
        if (null == dateUnit) {
            addException("datepart # " + datePart, locate(ctx));
        }
        function.setDateUnit(dateUnit);
        visit(ctx.expression());
        function.setExpr(popStatement());
        pushStatement(function);
        return function;
    }

    @Override
    public Object visitIdentity_function(TSqlParser.Identity_functionContext ctx) {
        return super.visitIdentity_function(ctx);
    }

    @Override
    public Object visitMin_active_rowversion_function(TSqlParser.Min_active_rowversion_functionContext ctx) {
        return super.visitMin_active_rowversion_function(ctx);
    }

    @Override
    public BaseFunction visitNullif_function(TSqlParser.Nullif_functionContext ctx) {
        NullIfFunction function = new NullIfFunction(new FuncName(null, "NULLIF", null));
        visit(ctx.expression().get(0));
        function.setLeftExpr(popStatement());
        visit(ctx.expression().get(1));
        function.setRightExpr(popStatement());
        pushStatement(function);
        return function;
    }

    @Override
    public TreeNode visitLeft_function(TSqlParser.Left_functionContext ctx) {
        LeftFunction function = new LeftFunction(new FuncName(null, "left", null));
        List<TreeNode> exprList = new ArrayList<TreeNode>();
        visit(ctx.expression().get(0));
        exprList.add(popStatement());
        visit(ctx.expression().get(1));
        exprList.add(popStatement());
        function.setExprList(exprList);
        pushStatement(function);
        return function;
    }

    @Override
    public TreeNode visitRight_function(TSqlParser.Right_functionContext ctx) {
        RightFunction function = new RightFunction(new FuncName(null, "right", null));
        List<TreeNode> exprList = new ArrayList<TreeNode>();
        visit(ctx.expression().get(0));
        exprList.add(popStatement());
        visit(ctx.expression().get(1));
        exprList.add(popStatement());
        function.setExprList(exprList);
        pushStatement(function);
        return function;
    }

    @Override
    public TreeNode visitIsnull_function(TSqlParser.Isnull_functionContext ctx) {
        IsNullFunction func = new IsNullFunction(new FuncName(null, "ISNULL", null));
        List<TSqlParser.ExpressionContext> list = ctx.expression();
        if (list.size() != 2) {
            addException("ISNULL function need 2 args", locate(ctx));
        }
        List<TreeNode> exprList = new ArrayList<TreeNode>();
        visit(list.get(0));
        exprList.add(popStatement());
        visit(list.get(1));
        exprList.add(popStatement());
        func.setExprList(exprList);
        pushStatement(func);
        return func;
    }

    @Override
    public Object visitIif_function(TSqlParser.Iif_functionContext ctx) {
        IifFunction func = new IifFunction(new FuncName(null, "IIF", null));
        visit(ctx.search_condition());
        TreeNode condition = popStatement();
        List<TSqlParser.ExpressionContext> exprList = ctx.expression();
        if (exprList.size() != 2)
            addException("IIF funciton need 3 args", locate(ctx));
        List<TreeNode> argList = new ArrayList<>();
        argList.add(condition);
        for (TSqlParser.ExpressionContext expressionContext : exprList) {
            visit(expressionContext);
            argList.add(popStatement());
        }
        func.setExprList(argList);
        pushStatement(func);
        return func;
    }

    @Override
    public Object visitTrim_function(TSqlParser.Trim_functionContext ctx) {
        TrimFunction trimFunc = new TrimFunction(new FuncName(null, "TRIM", null));
        List<TreeNode> exprList = new ArrayList<TreeNode>();
        List<TSqlParser.ExpressionContext> list = ctx.expression();
        for (TSqlParser.ExpressionContext exprCtx : list) {
            visit(exprCtx);
            exprList.add(popStatement());
        }
        trimFunc.setExprList(exprList);
        pushStatement(trimFunc);
        return trimFunc;
    }

    @Override
    public Object visitSession_user(TSqlParser.Session_userContext ctx) {
        return super.visitSession_user(ctx);
    }

    @Override
    public Object visitSystem_user(TSqlParser.System_userContext ctx) {
        return super.visitSystem_user(ctx);
    }

//    @Override
//    public BaseFunction visitFunction_call(TSqlParser.Function_callContext ctx) {
//        if() {
//
//        }


    //For 2017/1/13 before
//        SqlStatement rs = new SqlStatement();
//        if (null != ctx.ranking_windowed_function()) {
//            rs.sql.append(visitRanking_windowed_function(ctx.ranking_windowed_function()));
//        }
//        if (null != ctx.aggregate_windowed_function()) {
//            rs.sql.append(visitAggregate_windowed_function(ctx.aggregate_windowed_function()));
//        }
//        if (null != ctx.scalar_function_name()) {
//            // FunctionBean functionBean = (FunctionBean) visitScalar_function_name(ctx.scalar_function_name()).getData();
//            //TODO set function args
//            //functionBean.setArgs(null);
//            // FunctionStatement functionStatement = new FunctionStatement(functionBean);
//
//            rs.sql.append(visitExpression_list(ctx.expression_list()));
//        }
//        if (null != ctx.CAST()) {
//            rs.sql.append(ctx.CAST().getText());
//            rs.sql.append("(");
//            rs.sql.append(getExpressionSql(ctx.expression(0)));
//            rs.sql.append(" as ");
//            //TODO visit'Data'Type
//            // rs.appendSql(visitData_type(ctx.data_type()));
//            rs.sql.append(")");
//        }
//        if (null != ctx.CONVERT()) {
//
//            addException("CONVERT", locate(ctx));
//        }
//        if (null != ctx.CHECKSUM()) {
//            //用于校验列值是否改变
//            addException("CHECKSUM is not supported yet.\n ");
//        }
//        if (null != ctx.BINARY_CHECKSUM()) {
//            addException("BINARY_CHECKSUM is not supported yet.\n ");
//        }
//        if (null != ctx.COALESCE()) {
//            //COALESCE()函数可以接受一系列的值，如果列表中所有项都为空(null)，那么只使用一个值
//            rs.warnMsg.append("COALESCE is not supported yet.\n ");
//        }
//        if (null != ctx.CURRENT_TIMESTAMP()) {
//            rs.sql.append(" current_timestamp() ");
//        }
//        if (null != ctx.CURRENT_USER()) {
//            rs.sql.append(" CURRENT_USER() ");
//        }
//        if (null != ctx.DATEADD()) {
//            if (ctx.ID().toString().trim().equalsIgnoreCase("day")) {
//                rs.sql.append(" date_add(");
//                rs.sql.append(getExpressionSql(ctx.expression(1)));
//                rs.sql.append(",");
//                rs.sql.append(getExpressionSql(ctx.expression(0)));
//                rs.sql.append(" )");
//            } else {
//                addException("DATEADD noly supported add Day \n ");
//            }
//        }
//        if (null != ctx.DATEDIFF()) {
//            if (ctx.ID().toString().trim().equalsIgnoreCase("day")) {
//                rs.sql.append(" datediff(");
//                rs.sql.append(getExpressionSql(ctx.expression(0)));
//                rs.sql.append(",");
//                rs.sql.append(getExpressionSql(ctx.expression(1)));
//                rs.sql.append(" )");
//            } else {
//                addException("DATEDIFF noly supported add Day \n ");
//            }
//        }
//        if (null != ctx.DATENAME() || null != ctx.DATEPART()) {
//            String id = ctx.ID().toString().trim().toLowerCase();
//            if (StringUtils.equals(id, Common.DATE_FUCTION_YEAR)) {
//                rs.sql.append(" year(");
//                rs.sql.append(getExpressionSql(ctx.expression(0)));
//                rs.sql.append(" )");
//            } else if (StringUtils.equals(id, Common.DATE_FUCTION_MONTH)) {
//                rs.sql.append(" month(");
//                rs.sql.append(getExpressionSql(ctx.expression(0)));
//                rs.sql.append(" )");
//            } else if (StringUtils.equals(id, Common.DATE_FUCTION_DAY)) {
//                rs.sql.append(" day(");
//                rs.sql.append(getExpressionSql(ctx.expression(0)));
//                rs.sql.append(" )");
//            } else if (StringUtils.equals(id, Common.DATE_FUCTION_HOUR)) {
//                rs.sql.append(" hour(");
//                rs.sql.append(getExpressionSql(ctx.expression(0)));
//                rs.sql.append(" )");
//            } else if (StringUtils.equals(id, Common.DATE_FUCTION_MIN)) {
//                rs.sql.append(" minute(");
//                rs.sql.append(getExpressionSql(ctx.expression(0)));
//                rs.sql.append(" )");
//            } else if (StringUtils.equals(id, Common.DATE_FUCTION_SECOND)) {
//                rs.sql.append(" second(");
//                rs.sql.append(getExpressionSql(ctx.expression(0)));
//                rs.sql.append(" )");
//            } else if (StringUtils.equals(id, Common.DATE_FUCTION_WEEKOFYEAR)) {
//                rs.sql.append(" weekofyear(");
//                rs.sql.append(getExpressionSql(ctx.expression(0)));
//                rs.sql.append(" )");
//            } else {
//                addException(id + " is not supported yet \n ");
//            }
//        }
//        if (null != ctx.IDENTITY()) {
//            addException(ctx.IDENTITY() + " is not supported yet \n ");
//        }
//        if (null != ctx.MIN_ACTIVE_ROWVERSION()) {
//            addException(ctx.MIN_ACTIVE_ROWVERSION() + " is not supported yet \n ");
//        }
//        if (null != ctx.NULLIF()) {
//            rs.sql.append(" nullif(");
//            rs.sql.append(getExpressionSql(ctx.expression(0)));
//            rs.sql.append(",");
//            rs.sql.append(getExpressionSql(ctx.expression(1)));
//            rs.sql.append(" )");
//        }
//        if (null != ctx.CURRENT_USER()) {
//            addException(ctx.CURRENT_USER() + " is not supported yet \n ");
//        }
//        if (null != ctx.SYSTEM_USER()) {
//            addException(ctx.SYSTEM_USER() + " is not supported yet \n ");
//        }
//        return rs;

//        return null;
//
//    }


    @Override
    public TreeNode visitRanking_windowed_function(TSqlParser.Ranking_windowed_functionContext ctx) {
        StringBuffer sb = new StringBuffer();

        if (null != ctx.RANK()) {
            sb.append(ctx.RANK().getText());
        }
        if (null != ctx.DENSE_RANK()) {
            sb.append(ctx.DENSE_RANK().getText());
        }
        if (null != ctx.ROW_NUMBER()) {
            sb.append(ctx.ROW_NUMBER().getText());
        }
        String funcName = sb.toString();
        if (sb.length() > 0) {
            sb.append("() ");
        }
        if (null != ctx.NTILE()) {
            sb.append(ctx.NTILE().getText());
            sb.append("(");
            sb.append(getExpressionSql(ctx.expression()));
            sb.append(") ");
            funcName = ctx.NTILE().getText();
        }

        if (null != ctx.over_clause()) {
            sb.append(visitOver_clause(ctx.over_clause()));
        }

//        popStatement();

//        sb.append(visitOver_clause(ctx.over_clause()));
        RankWindowFunction function = new RankWindowFunction(new FuncName(null, funcName, null));
        sb.append(" ");
        function.setSql(sb.toString());
        pushStatement(function);
        return function;
    }

    @Override
    public String visitOver_clause(TSqlParser.Over_clauseContext ctx) {
        StringBuffer rs = new StringBuffer();
        rs.append(ctx.OVER().getText()).append(Common.SPACE);
        rs.append("(");
        if (null != ctx.PARTITION()) {
            rs.append(ctx.PARTITION().getText()).append(Common.SPACE);
            rs.append(ctx.BY().getText()).append(Common.SPACE);
            popStatement();
            rs.append(visitExpression_list(ctx.expression_list()).getSql());
        }
        if (null != ctx.order_by_clause()) {
            rs.append(visitOrder_by_clause(ctx.order_by_clause()));
        }
        if (null != ctx.row_or_range_clause()) {
            rs.append(visitRow_or_range_clause(ctx.row_or_range_clause()));
        }
        rs.append(")");
        return rs.toString();
    }


    @Override
    public TreeNode visitExpression_list(TSqlParser.Expression_listContext ctx) {
        ExpressionListStatement exprs = new ExpressionListStatement();
        List<TreeNode> rs = new ArrayList<>();
        for (TSqlParser.ExpressionContext expressionContext : ctx.expression()) {
            visit(expressionContext);
            rs.add(popStatement());
        }
        exprs.addExpresses(rs);
//        exprs.setSql(getExpressionListSql(rs));
//        exprs.setSql();
        addNode(exprs);
        pushStatement(exprs);
        return exprs;
    }

//    @Override
//    public BaseFunction visitFunction_call_expression(TSqlParser.Function_call_expressionContext ctx) {
//        visitChildren(ctx);
//        return null;
//    }

    @Override
    public SqlStatement visitCase_expression(TSqlParser.Case_expressionContext ctx) {
        CaseWhenStatement caseWhenStatement = new CaseWhenStatement();
        StringBuffer sql = new StringBuffer();
        sql.append(ctx.CASE().getText()).append(Common.SPACE);
        if (!ctx.switch_search_condition_section().isEmpty()) {
            //====type=1 搜索表达式
            caseWhenStatement.setCaseWhenStatementType(1);
            for (int i = 0; i < ctx.switch_search_condition_section().size(); i++) {
                sql.append(visitSwitch_search_condition_section(ctx.switch_search_condition_section(i)).getSql());
                addNode(caseWhenStatement);
            }
            if (null != ctx.ELSE()) {
                sql.append(ctx.ELSE().getText()).append(Common.SPACE);
                TreeNode elseSqlStatement = (TreeNode) visit(ctx.expression(0));
                elseSqlStatement.setNodeType(TreeNode.Type.ELSE);
                sql.append(elseSqlStatement.getSql());
                addNode(caseWhenStatement);
            }
        } else {
            //====type=0 简单表达式
            caseWhenStatement.setCaseWhenStatementType(0);
            TreeNode sqlStatement = (TreeNode) visit(ctx.expression(0));
            sqlStatement.setNodeType(TreeNode.Type.CASE_INPUT);
            addNode(caseWhenStatement);
            sql.append(sqlStatement.getSql());


            for (int i = 0; i < ctx.switch_section().size(); i++) {
                sql.append(visitSwitch_section(ctx.switch_section(i)).getSql());
                addNode(caseWhenStatement);
            }
            if (ctx.expression().size() > 1) {
                sql.append(ctx.ELSE().getText()).append(Common.SPACE);
                TreeNode elseSqlStatement = (TreeNode) visit(ctx.expression(1));
                elseSqlStatement.setNodeType(TreeNode.Type.ELSE);
                sql.append(elseSqlStatement.getSql());
                addNode(caseWhenStatement);
            }
        }
        sql.append(ctx.END().getText()).append(Common.SPACE);
        caseWhenStatement.setSql(sql.toString());
        pushStatement(caseWhenStatement);
        return caseWhenStatement;

    }


    @Override
    public SqlStatement visitSwitch_section(TSqlParser.Switch_sectionContext ctx) {
        SwichStatement swichStatement = new SwichStatement();
        StringBuffer sql = new StringBuffer();
        swichStatement.setNodeType(TreeNode.Type.SWICH);
        sql.append(ctx.WHEN().getText()).append(Common.SPACE);
        SqlStatement whenSqlStatement = (SqlStatement) visit(ctx.expression(0));
        whenSqlStatement.setNodeType(TreeNode.Type.WHEN);
        sql.append(whenSqlStatement.getSql());
        addNode(swichStatement);
        sql.append(ctx.THEN().getText()).append(Common.SPACE);

        SqlStatement thenSqlStatement = (SqlStatement) visit(ctx.expression(1));
        thenSqlStatement.setNodeType(TreeNode.Type.THEN);
        sql.append(thenSqlStatement.getSql());
        swichStatement.setSql(sql.toString());
        addNode(swichStatement);

        pushStatement(swichStatement);
        return swichStatement;
    }


    @Override
    public SqlStatement visitSwitch_search_condition_section(TSqlParser.Switch_search_condition_sectionContext ctx) {
        SwichStatement swichStatement = new SwichStatement();
        StringBuffer sql = new StringBuffer();
        swichStatement.setNodeType(TreeNode.Type.SWICH);
        sql.append(ctx.WHEN().getText()).append(Common.SPACE);
        LogicNode logicNode = visitSearch_condition(ctx.search_condition());
        /*logicNode.setNodeType(TreeNode.Type.WHEN)*/
        ;
        sql.append(logicNode.toString());
        addNode(swichStatement);
        popStatement();
        sql.append(ctx.THEN().getText()).append(Common.SPACE);
        TreeNode sqlStatement = (TreeNode) visit(ctx.expression());
        sqlStatement.setNodeType(TreeNode.Type.THEN);
        sql.append(sqlStatement.getSql());
        swichStatement.setSql(sql.toString());
        addNode(swichStatement);
        pushStatement(swichStatement);
        return swichStatement;
    }

    @Override
    public String visitSearch_condition_list(TSqlParser.Search_condition_listContext ctx) {
        StringBuffer rs = new StringBuffer();
        for (int i = 0; i < ctx.search_condition().size(); i++) {
            if (i != 0) {
                rs.append(",").append(Common.SPACE);
            }
            rs.append(visitSearch_condition(ctx.search_condition(i))).append(Common.SPACE);
            popStatement();
        }
        return rs.toString();
    }

    @Override
    public String visitComparison_operator(TSqlParser.Comparison_operatorContext ctx) {
        return ctx.getText();
    }

    @Override
    public String visitNull_notnull(TSqlParser.Null_notnullContext ctx) {
        StringBuffer rs = new StringBuffer();
        if (null != ctx.NOT()) {
            rs.append(ctx.NOT().getText()).append(Common.SPACE);
        }
        rs.append(ctx.NULL()).append(Common.SPACE);
        return rs.toString();
    }

    @Override
    public SqlStatement visitColumn_ref_expression(TSqlParser.Column_ref_expressionContext ctx) {
        SqlStatement sqlStatement = visitFull_column_name(ctx.full_column_name());
        pushStatement(sqlStatement);
        return sqlStatement;
    }

    @Override
    public SqlStatement visitFull_column_name(TSqlParser.Full_column_nameContext ctx) {
        this.popStatement();
        SqlStatement rs = new SqlStatement();
        StringBuffer sql = new StringBuffer();
        if (null != ctx.table_name()) {
            sql.append(visitTable_name(ctx.table_name()).getRealFullFuncName());
            sql.append(".");
        }
        sql.append(StrUtils.replaceAllBracketToQuit(visitId(ctx.id()))).append(Common.SPACE);
        rs.setSql(sql.toString());
        return rs;

    }

    @Override
    public SqlStatement visitBracket_expression(TSqlParser.Bracket_expressionContext ctx) {
        ExpressionBean expressionBean = new ExpressionBean();
        ExpressionStatement es = new ExpressionStatement(expressionBean);
        expressionBean.setOperatorSign(OperatorSign.BRACKET);
        StringBuffer sql = new StringBuffer();

        sql.append("(").append(Common.SPACE);
        sql.append(getExpressionSql(ctx.expression()));
        sql.append(")").append(Common.SPACE);
        es.setSql(sql.toString());
        addNode(es);
        pushStatement(es);
        return es;
    }

    @Override
    public SqlStatement visitSubquery_expression(TSqlParser.Subquery_expressionContext ctx) {
        SubqueryStatement subqueryStatement = new SubqueryStatement();
        StringBuffer sql = new StringBuffer();
        sql.append("(");
        sql.append(visitSubquery(ctx.subquery()).getSql());
        sql.append(")");
        subqueryStatement.setSql(sql.toString());
        addNode(subqueryStatement);
        pushStatement(subqueryStatement);
        return subqueryStatement;
    }

    @Override
    public SqlStatement visitUnary_operator_expression(TSqlParser.Unary_operator_expressionContext ctx) {
        ExpressionBean expressionBean = new ExpressionBean();
        ExpressionStatement expressionStatement = new ExpressionStatement(expressionBean);
        StringBuffer sql = new StringBuffer();
        if (null != ctx.op) {
            sql.append(ctx.op.getText());
            expressionBean.setOperatorSign(OperatorSign.getOpator(ctx.op.getText()));
        } else {
            sql.append("~");
            expressionBean.setOperatorSign(OperatorSign.BIT_NOT);
        }
        TreeNode ss = (TreeNode) visit(ctx.expression());
        addNode(expressionStatement);
        sql.append(ss.getSql());
        expressionStatement.setSql(sql.toString());
        pushStatement(expressionStatement);
        return expressionStatement;
    }

    @Override
    public SqlStatement visitBinary_operator_expression(TSqlParser.Binary_operator_expressionContext ctx) {
        ExpressionBean expressionBean = new ExpressionBean();
        ExpressionStatement es = new ExpressionStatement(expressionBean);
        StringBuffer sql = new StringBuffer();
        if (ctx.expression().size() == 2) {
            if (null != ctx.comparison_operator()) {
                expressionBean.setOperatorSign(OperatorSign.getOpator(ctx.comparison_operator().getText()));
                sql.append(getExpressionSql(ctx.expression(0)));
                addNode(es);
                sql.append(ctx.comparison_operator().getText());
                sql.append(getExpressionSql(ctx.expression(1)));
                addNode(es);
            } else {
                expressionBean.setOperatorSign(OperatorSign.getOpator(ctx.op.getText()));
                sql.append(getExpressionSql(ctx.expression(0)));
                addNode(es);
                sql.append(ctx.op.getText());
                sql.append(getExpressionSql(ctx.expression(1)));
                addNode(es);
            }
        } else {
            expressionBean.setOperatorSign(OperatorSign.getOpator(ctx.op.getText()));
            sql.append(ctx.op.getText());
            sql.append(getExpressionSql(ctx.expression(0)));
            addNode(es);
        }
        es.setSql(sql.toString());
        pushStatement(es);
        return es;
    }

    @Override
    public SqlStatement visitOver_clause_expression(TSqlParser.Over_clause_expressionContext ctx) {
        SqlStatement sqlStatement = new SqlStatement();
        sqlStatement.setSql(visitOver_clause(ctx.over_clause()));
        return sqlStatement;
    }

    @Override
    public SqlStatement visitPrimitive_expression(TSqlParser.Primitive_expressionContext ctx) {
        ExpressionBean expressionBean = new ExpressionBean();
        StringBuffer sql = new StringBuffer();
        Var var = new Var();
        if (null != ctx.NULL()) {
            sql.append(ctx.NULL().getText());
            var.setDataType(Var.DataType.NULL);
            var.setVarValue(ctx.NULL().getText());

        }
        if (null != ctx.DEFAULT()) {
            sql.append(ctx.DEFAULT().getText());
            var.setDataType(Var.DataType.DEFAULT);
            var.setVarValue(ctx.NULL().getText());
        }
        if (null != ctx.LOCAL_ID()) {
            //添加变量
            //如sql中WHERE OrganizationLevel > @cond AND OrganizationLevel < @cond sql中的@cond
            String variableName = ctx.LOCAL_ID().getText();
            localIdVariable.add(variableName);
            sql.append(ctx.LOCAL_ID().getText());
            var.setDataType(Var.DataType.VAR);
            var.setVarName(ctx.LOCAL_ID().getText());
        }
        if (null != ctx.constant()) {
            var = visitConstant(ctx.constant());
            try {
                sql.append(var.getVarValue());
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
        expressionBean.setVar(var);
        ExpressionStatement expressionStatement = new ExpressionStatement(expressionBean);
        sql.append(SPACE);
        expressionStatement.setSql(sql.toString());
        this.pushStatement(expressionStatement);
        return expressionStatement;
    }

    private final static String SPACE = " ";

    @Override
    public String visitOrder_by_clause(TSqlParser.Order_by_clauseContext ctx) {
        StringBuffer sql = new StringBuffer();
        sql.append(ctx.ORDER().getText()).append(Common.SPACE);
        sql.append(ctx.BY().getText()).append(Common.SPACE);
        for (int i = 0; i < ctx.order_by_expression().size(); i++) {
            if (i != 0) {
                sql.append(",").append(Common.SPACE);
            }
            sql.append(visitOrder_by_expression(ctx.order_by_expression(i)));
        }
        // (OFFSET expression (ROW | ROWS) (FETCH (FIRST | NEXT) expression (ROW | ROWS) ONLY)?)?
        if (null != ctx.OFFSET()) {
            addException("[OFFSET  FETCH NEXT ONLY ]", locate(ctx));
        }
        return sql.toString();
    }


    @Override
    public String visitOrder_by_expression(TSqlParser.Order_by_expressionContext ctx) {
        StringBuffer rs = new StringBuffer();
        rs.append(getExpressionSql(ctx.expression()));
        popStatement();
        if (null != ctx.ASC()) {
            rs.append(Common.SPACE).append(ctx.ASC()).append(Common.SPACE);
        }
        if (null != ctx.DESC()) {
            rs.append(Common.SPACE).append(ctx.DESC()).append(Common.SPACE);
        }
        return rs.toString();
    }

    @Override
    public String visitRow_or_range_clause(TSqlParser.Row_or_range_clauseContext ctx) {
        StringBuffer stringBuffer = new StringBuffer();
        if (null != ctx.ROWS()) {
            stringBuffer.append(ctx.ROWS().getText()).append(Common.SPACE);
        }
        if (null != ctx.RANGE()) {
            stringBuffer.append(ctx.RANGE().getText()).append(Common.SPACE);
        }
        stringBuffer.append(visitWindow_frame_extent(ctx.window_frame_extent()));
        return stringBuffer.toString();
    }


    @Override
    public String visitWindow_frame_extent(TSqlParser.Window_frame_extentContext ctx) {
        StringBuffer rs = new StringBuffer();
        if (null != ctx.window_frame_preceding()) {
            rs.append(visitWindow_frame_preceding(ctx.window_frame_preceding()).getSql());
        }
        if (null != ctx.BETWEEN()) {
            rs.append(ctx.BETWEEN().getText()).append(Common.SPACE);
        }
        rs.append(visitWindow_frame_bound(ctx.window_frame_bound(0)).getSql());
        rs.append(Common.SPACE).append(ctx.AND().getText()).append(Common.SPACE);
        rs.append(visitWindow_frame_bound(ctx.window_frame_bound(1)).getSql());
        return rs.toString();
    }


    @Override
    public SqlStatement visitWindow_frame_bound(TSqlParser.Window_frame_boundContext ctx) {
        SqlStatement rs = new SqlStatement();
        if (null != ctx.window_frame_preceding()) {
            rs = visitWindow_frame_preceding(ctx.window_frame_preceding());
        }
        if (null != ctx.window_frame_following()) {
            rs = visitWindow_frame_following(ctx.window_frame_following());
        }
        return rs;
    }

    @Override
    public SqlStatement visitWindow_frame_preceding(TSqlParser.Window_frame_precedingContext ctx) {
        SqlStatement rs = new SqlStatement();
        StringBuffer sql = new StringBuffer();


        if (null != ctx.UNBOUNDED()) {
            sql.append(ctx.UNBOUNDED().getText()).append(Common.SPACE);
            sql.append(ctx.PRECEDING().getText()).append(Common.SPACE);
        }
        if (null != ctx.DECIMAL()) {
            sql.append(ctx.DECIMAL().getText()).append(Common.SPACE);
            sql.append(ctx.PRECEDING().getText()).append(Common.SPACE);
        }
        if (null != ctx.CURRENT()) {
            sql.append(ctx.CURRENT().getText()).append(Common.SPACE);
            sql.append(ctx.ROW().getText()).append(Common.SPACE);
        }
        rs.setSql(sql.toString());
        return rs;
    }

    @Override
    public SqlStatement visitWindow_frame_following(TSqlParser.Window_frame_followingContext ctx) {
        SqlStatement rs = new SqlStatement();
        StringBuffer sql = new StringBuffer();
        if (null != ctx.UNBOUNDED()) {
            sql.append(ctx.UNBOUNDED().getText()).append(Common.SPACE);
        }
        if (null != ctx.DECIMAL()) {
            sql.append(ctx.DECIMAL().getText()).append(Common.SPACE);
        }
        sql.append(ctx.FOLLOWING().getText()).append(Common.SPACE);
        rs.setSql(sql.toString());
        return rs;
    }

    @Override
    public SqlStatement visitChange_table(TSqlParser.Change_tableContext ctx) {
        SqlStatement rs = new SqlStatement();
        //TODO 返回表更改的跟踪信息
        return rs;
    }


    @Override
    public String visitDerived_table(TSqlParser.Derived_tableContext ctx) {
        StringBuffer rs = new StringBuffer();
        rs.append("( ");
        rs.append(visitSubquery(ctx.subquery()).getSql());
        rs.append(" )");
        popStatement();
        return rs.toString();
    }

    @Override
    public SqlStatement visitSubquery(TSqlParser.SubqueryContext ctx) {
        SubqueryStatement subqueryStatement = new SubqueryStatement();
        subqueryStatement.setSql(visitSelect_statement(ctx.select_statement()).getSql());
        addNode(subqueryStatement);
        pushStatement(subqueryStatement);
        return subqueryStatement;
    }

    @Override
    public String visitRowset_function(TSqlParser.Rowset_functionContext ctx) {
        //TODO 访问远程数据库字符串
        addException("rowSet function ", locate(ctx));
        return "";
    }

    private boolean withFlag = false;

    /**
     * 只有在update,del時才替換
     *
     * @param ctx
     * @return
     */

    @Override
    public String visitTable_name_with_hint(TSqlParser.Table_name_with_hintContext ctx) {
        StringBuffer rs = new StringBuffer();
        String tableName = visitTable_name(ctx.table_name()).getRealFullFuncName();


        /**
         * 如果写了with表达式，将替换为子查询，如果没有则直接写表名
         */
        if (withExpressionSqlMap.containsKey(tableName) && withFlag) {
            rs.append("( ");
            rs.append(withExpressionSqlMap.get(tableName));
            rs.append(") ");
            // rs.append(tableName);
        } else {
            rs.append(tableName);
        }
        if (null != ctx.AS()) {
            rs.append(" AS ");
        }
        if (null != ctx.id()) {
            rs.append(Common.SPACE);
            rs.append(visitId(ctx.id())).append(Common.SPACE);
        }
        if (null != ctx.with_table_hints_lock_table()) {
            rs.append(visitWith_table_hints_lock_table(ctx.with_table_hints_lock_table()));
        }
        return rs.toString();
    }

    @Override
    public String visitWith_table_hints(TSqlParser.With_table_hintsContext ctx) {
        StringBuffer rs = new StringBuffer();
        if (null != ctx.WITH()) {
            rs.append(ctx.WITH().getText()).append(Common.SPACE);
        }
        rs.append("(");
        if (!ctx.table_hint().isEmpty()) {
            for (int i = 0; i < ctx.table_hint().size(); i++) {
                if (i != 0) {
                    rs.append(",");
                }
                rs.append(visitTable_hint(ctx.table_hint(i)));
            }
        }
        rs.append(")");
        return rs.toString();
    }

    @Override
    public String visitTable_hint(TSqlParser.Table_hintContext ctx) {
        //TODO LOCK TABLE
        //addException("table hint " + ctx.getText(), locate(ctx));
        return "";
    }

    @Override
    public String visitWith_table_hints_lock_table(TSqlParser.With_table_hints_lock_tableContext ctx) {
        //addException("table hint " + ctx.getText(), locate(ctx));
        return "";
    }

    @Override
    public String visitAs_table_alias(TSqlParser.As_table_aliasContext ctx) {

        StringBuffer rs = new StringBuffer();
        if (null != ctx.AS()) {
            rs.append(Common.SPACE).append(ctx.AS().getText()).append(Common.SPACE);
        }
        rs.append(visitTable_alias(ctx.table_alias()));
        return rs.toString();
    }

    @Override
    public String visitTable_alias(TSqlParser.Table_aliasContext ctx) {
        StringBuffer rs = new StringBuffer();
        rs.append(Common.SPACE).append(visitId(ctx.id())).append(Common.SPACE);
        if (null != ctx.with_table_hints()) {
            rs.append(visitWith_table_hints(ctx.with_table_hints()));
        }

        return rs.toString();
    }

    @Override
    public String visitJoin_part(TSqlParser.Join_partContext ctx) {
        StringBuffer rs = new StringBuffer();
        rs.append(Common.SPACE);
        if (null != ctx.INNER()) {
            rs.append(ctx.INNER()).append(Common.SPACE);
        }
        if (null != ctx.LEFT()) {
            rs.append(ctx.LEFT()).append(Common.SPACE);
        }
        if (null != ctx.RIGHT()) {
            rs.append(ctx.RIGHT()).append(Common.SPACE);
        }
        if (null != ctx.FULL()) {
            rs.append(ctx.FULL()).append(Common.SPACE);
        }
        if (null != ctx.OUTER()) {
            rs.append(ctx.OUTER()).append(Common.SPACE);
        }
        if (null != ctx.join_hint) {
            addException(" joinType :" + ctx.join_hint.getText(), locate(ctx));
        }
        rs.append(ctx.JOIN().getText()).append(Common.SPACE);
        rs.append(visitTable_source(ctx.table_source())).append(Common.SPACE);
        rs.append(ctx.ON()).append(Common.SPACE);
        LogicNode logicNode = visitSearch_condition(ctx.search_condition());
        if (null != logicNode) {
            rs.append(logicNode.toString()).append(Common.SPACE);
            popStatement();
        }
        if (null != ctx.CROSS()) {
            rs.append(ctx.CROSS().getText()).append(Common.SPACE);
            if (null != ctx.JOIN()) {
                rs.append(ctx.JOIN().getText()).append(Common.SPACE);
            }
            rs.append(visitTable_source(ctx.table_source())).append(Common.SPACE);
        }
        if (null != ctx.APPLY()) {
            addException("APPLY", locate(ctx));
        }
        return rs.toString();
    }

    @Override
    public ArrayList<ColumnBean> visitSelect_list(TSqlParser.Select_listContext ctx) {
        ArrayList<ColumnBean> columns = new ArrayList<>();
        if (!ctx.select_list_elem().isEmpty()) {
            for (int i = 0; i < ctx.select_list_elem().size(); i++) {
                columns.add(visitSelect_list_elem(ctx.select_list_elem(i)));
            }
        }
        return columns;
    }


    /**
     * select_list_elem
     * : (table_name '.')? ('*' | '$' (IDENTITY | ROWGUID))
     * | column_alias '=' expression
     * | expression (AS? column_alias)?
     *
     * @param ctx
     * @return
     */
    @Override
    public ColumnBean visitSelect_list_elem(TSqlParser.Select_list_elemContext ctx) {
        ColumnBean columnBean = new ColumnBean();
        StringBuffer rs = new StringBuffer();
        if (StringUtils.contains(ctx.getText(), "=") && null != ctx.column_alias()) {
            // | column_alias '=' expression 解析为 select expression as column_alias
            rs.append(getExpressionSql(ctx.expression()));
            rs.append("  as ");
            String columnAlias = visitColumn_alias(ctx.column_alias());
            rs.append(columnAlias).append(Common.SPACE);
            popStatement();
            columnBean.setSql(rs.toString());
            return columnBean;
        }
        if (null != ctx.table_name()) {
            String tableName = visitTable_name(ctx.table_name()).getRealFullFuncName();
            rs.append(tableName).append(".");
        }
        if (null != ctx.IDENTITY() || null != ctx.ROWGUID()) {
            addException(" IDENTITY, ROWGUID", locate(ctx));
        }

        if (null != ctx.expression()) {
            /**
             * 添加保存将执行sql的结果的变量
             * 如select  @aa=string2  from boolean_ana where string1 ='code' 中的@aa
             */
            checkResultVariable(rs, ctx.expression(), columnBean);

            /** 解决with中有别名 bug LEAP-2647
             *with t_bTemp(i,n,a)
             *as (select b1.id, b1.name, b2.age from b1 inner join b2 on b1.id = b2.id)
             *insert into b3 select i,n,a from t_bTemp
             **/

            if (!withColmnNameAlias.isEmpty()) {
                rs.append(Common.SPACE).append("as ").append(withColmnNameAlias.poll()).append(Common.SPACE);
            } else {
                if (null != ctx.AS()) {
                    rs.append(ctx.AS().getText()).append(Common.SPACE);
                }
                if (null != ctx.column_alias()) {
                    String colAlias = visitColumn_alias(ctx.column_alias());
                    rs.append(colAlias).append(Common.SPACE);
                    columnBean.setColumnAlias(colAlias);
                }
            }
        }
        if (ctx.column_alias() == null && ctx.expression() == null && (null != ctx.IDENTITY() || null != ctx.ROWGUID())) {
            addException("IDENTITY. ROWGUID", locate(ctx));
        }
        if (ctx.column_alias() == null && ctx.expression() == null && null == ctx.IDENTITY() && null == ctx.IDENTITY()) {
            if (!withColmnNameAlias.isEmpty()) {
                //不支持 with tmpTable(col,col2) as select * from table1 insert into select * from tmpTable"
                addException("with sql has columns ", locate(ctx));
            } else {
                rs.append("*").append(Common.SPACE);
            }
        }
        columnBean.setSql(rs.toString());
        return columnBean;
    }


    private void checkResultVariable(StringBuffer stringBuffer, TSqlParser.ExpressionContext expressionContext, ColumnBean columnBean) {
        //必须符合这样的规则 expression comparison_operator expression
        if (expressionContext.getChildCount() == 3) {
            if (!expressionContext.getChild(1).getText().equals("=")) {
                visit(expressionContext);
                String expression = popStatement().getSql();
                columnBean.setCloumnName(expression);
                stringBuffer.append(expression);
                return;
            }
            String localIdVariableName = expressionContext.getChild(0).getText();
            if (localIdVariableName.contains("@")) {
                resultSetVariable.add(localIdVariableName);
                TreeNode exprStatement = (TreeNode) visit(expressionContext.getChild(2));
                popStatement();
                String expression = exprStatement.getSql();
                columnBean.setCloumnName(expression);
                stringBuffer.append(expression);
            }
        } else {
//            TreeNode expressionStatement = (TreeNode) visit(expressionContext);
            visit(expressionContext);
            String expression = popStatement().getSql();
            /**
             * 解决 select a1.*, NULL as Billing_Month, 1+2 as ss from a1
             * 列为null
             */
            if (StringUtils.isBlank(expression) || StringUtils.equals(expression.toUpperCase().trim(), "NULL")) {
                columnBean.setCloumnName("'' ");
                expression = "'' ";
            } else {
                columnBean.setCloumnName(expression);
            }
            stringBuffer.append(expression);
        }
    }


    @Override
    public UnionBean visitUnion(TSqlParser.UnionContext ctx) {
        UnionBean unionBean = new UnionBean();
        StringBuffer sql = new StringBuffer();
        if (null != ctx.UNION()) {
            unionBean.setUnionType(UnionBean.UnionType.UNION);
            sql.append(ctx.UNION().getText()).append(Common.SPACE);
        }
        if (null != ctx.ALL()) {
            sql.append(ctx.ALL().getText()).append(Common.SPACE);
        }
        if (null != ctx.EXCEPT()) {
            /*addException(ctx.EXCEPT().getText(), locate(ctx));*/
            unionBean.setUnionType(UnionBean.UnionType.EXCEPT);
        }
        if (null != ctx.INTERSECT()) {
           /* addException(ctx.INTERSECT().getText(), locate(ctx));*/
            unionBean.setUnionType(UnionBean.UnionType.INTERSECT);
        }
        if (null != ctx.query_specification()) {
            QuerySpecificationBean querySpecificationBean = visitQuery_specification(ctx.query_specification());
            unionBean.setQuerySpecificationBean(querySpecificationBean);
            String querySpecRs = querySpecificationBean.getSql();
            sql.append(querySpecRs);
        }
        if (!ctx.query_expression().isEmpty()) {
            for (int i = 0; i < ctx.query_expression().size(); i++) {
                sql.append("(");
                sql.append(visitQuery_expression(ctx.query_expression(i)).getSql());
                sql.append(")");
            }
        }
        unionBean.setSql(sql.toString());
        return unionBean;
    }

    @Override
    public String visitCrud_table(TSqlParser.Crud_tableContext ctx) {
        StringBuffer crud = new StringBuffer();
        crud.append("CLUSTERED BY (");
        crud.append(StrUtils.concat(visitColumn_name_list(ctx.column_name_list())));
        crud.append(")");
        crud.append(" INTO ");
        crud.append(ctx.DECIMAL().getText());
        crud.append(" BUCKETS STORED AS ORC TBLPROPERTIES (\"transactional\"=\"true\") ");
        return crud.toString();
    }


    private void checkLength(String str, int length, String warningMsg) {
        if (str.length() > length) {
            addException(new Exception(warningMsg + ":" + "[" + str + "]" + " is too long"));
        }
    }


    @Override
    public MergeIntoStatement visitMerge_statement(TSqlParser.Merge_statementContext ctx) {
        MergeIntoStatement mergeIntoStatement = new MergeIntoStatement();
        if (null != ctx.with_expression()) {
            WithExpressionBean withExpressionBean = visitWith_expression(ctx.with_expression());
            mergeIntoStatement.setWithExpressionBean(withExpressionBean);
        }
        if (null != ctx.TOP()) {
            if (null != ctx.PERCENT()) {
                addException("percent", locate(ctx));
            }
            visit(ctx.expression());
            LimitBean limitBean = new LimitBean();
            limitBean.setExpression(popStatement().getSql());
            limitBean.setSql(" limit " + limitBean.getExpression() + Common.SPACE);
        }
        FuncName targetTableName = visitTable_name(ctx.target_table);
        mergeIntoStatement.setTargetTableName(targetTableName);
        if (null != ctx.target_table_alias) {
            String targetTbAlias = visitAs_table_alias(ctx.target_table_alias).trim();
            if (targetTbAlias.startsWith("as ") || targetTbAlias.startsWith("AS ")) {
                targetTbAlias = targetTbAlias.substring(2, targetTbAlias.length()).trim();
            }
            mergeIntoStatement.setTargetTableAlias(targetTbAlias);
        }


        FuncName srcTableName = visitTable_name(ctx.src_table);
        mergeIntoStatement.setSrcTableName(srcTableName);
        if (null != ctx.src_table_alias) {
            String srcTbAlias = visitAs_table_alias(ctx.src_table_alias).trim();
            if (srcTbAlias.startsWith("as ") || srcTbAlias.startsWith("AS ")) {
                srcTbAlias = srcTbAlias.substring(2, srcTbAlias.length()).trim();
            }
            mergeIntoStatement.setSrcTableAlias(srcTbAlias);
        }

        String searchContion = visitSearch_condition(ctx.search_condition()).toString();
        mergeIntoStatement.setSearchCondition(searchContion);

        List<MatchedStatmentBean> statmentBeanList = new ArrayList<>();
        for (int i = 0; i < ctx.matched_statment().size(); i++) {
            MatchedStatmentBean matchedStatment = visitMatched_statment(ctx.matched_statment(i));
            statmentBeanList.add(matchedStatment);
        }
        mergeIntoStatement.setStatmentBeanList(statmentBeanList);
        List<TargetNotMatcheBean> targetNotMatcheBeanArrayList = new ArrayList<>();
        for (int i = 0; i < ctx.target_not_matched().size(); i++) {
            TargetNotMatcheBean targetNotMatcheBean = visitTarget_not_matched(ctx.target_not_matched(i));
            targetNotMatcheBeanArrayList.add(targetNotMatcheBean);
        }
        mergeIntoStatement.setTargetNotMatcheBeanArrayList(targetNotMatcheBeanArrayList);
        List<SourceNotMatchedBean> sourceNotMatchedBeans = new ArrayList<>();
        for (int i = 0; i < ctx.source_not_matched().size(); i++) {
            SourceNotMatchedBean sourceNotMatchedBean = visitSource_not_matched(ctx.source_not_matched(i));
            sourceNotMatchedBeans.add(sourceNotMatchedBean);
        }
        mergeIntoStatement.setSourceNotMatchedBeans(sourceNotMatchedBeans);
        pushStatement(mergeIntoStatement);
        return mergeIntoStatement;
    }


    @Override
    public MatchedStatmentBean visitMatched_statment(TSqlParser.Matched_statmentContext ctx) {
        MatchedStatmentBean matchedStatmentBean = new MatchedStatmentBean();
        if (null != ctx.search_condition()) {
            String searchCondition = visitSearch_condition(ctx.search_condition()).toString();
            matchedStatmentBean.setSearchCondition(searchCondition);
        }
        matchedStatmentBean.setMatchedBean(visitMerge_matched(ctx.merge_matched()));
        return matchedStatmentBean;

    }

    @Override
    public MatchedBean visitMerge_matched(TSqlParser.Merge_matchedContext ctx) {
        MatchedBean matchedBean = new MatchedBean();
        if (null != ctx.DELETE()) {
            matchedBean.setType(MatchedBean.MatchedBeanType.DEL);
        } else {
            matchedBean.setType(MatchedBean.MatchedBeanType.UPDATE);
            StringBuffer sql = new StringBuffer();
            sql.append(Common.SPACE).append(" set ");
            for (int i = 0; i < ctx.update_elem().size(); i++) {
                if (i != 0) {
                    sql.append(",");
                }
                sql.append(visitUpdate_elem(ctx.update_elem(i)));
            }
            matchedBean.setUpdateSetSql(sql.toString());
        }
        return matchedBean;
    }

    @Override
    public MergeNotMatchedBean visitMerge_not_matched(TSqlParser.Merge_not_matchedContext ctx) {
        MergeNotMatchedBean mergeNotMatchedBean = new MergeNotMatchedBean();
        if (null != ctx.column_name_list()) {
            List<String> list = visitColumn_name_list(ctx.column_name_list());
            mergeNotMatchedBean.setColumnNameList(list);
        }
        StringBuffer values = new StringBuffer();
        for (int i = 0; i < ctx.expression_list().size(); i++) {
            if (i != 0) {
                values.append(",");
            }
            values.append("(");
            values.append(visitExpression_list(ctx.expression_list(i)).getSql());
            popStatement();
            values.append(")");
        }
        mergeNotMatchedBean.setValues(values.toString());
        return mergeNotMatchedBean;
    }


    @Override
    public TargetNotMatcheBean visitTarget_not_matched(TSqlParser.Target_not_matchedContext ctx) {
        TargetNotMatcheBean targetNotMatcheBean = new TargetNotMatcheBean();
        if (null != ctx.search_condition()) {
            String searchCondition = visitSearch_condition(ctx.search_condition()).toString();
            targetNotMatcheBean.setSearchCondition(searchCondition);
        }
        targetNotMatcheBean.setMergeNotMatchedBean(visitMerge_not_matched(ctx.merge_not_matched()));
        return targetNotMatcheBean;
    }

    @Override
    public SourceNotMatchedBean visitSource_not_matched(TSqlParser.Source_not_matchedContext ctx) {
        SourceNotMatchedBean sourceNotMatchedBean = new SourceNotMatchedBean();
        if (null != ctx.search_condition()) {
            String searchCondition = visitSearch_condition(ctx.search_condition()).getSql();
            sourceNotMatchedBean.setSearchCondition(searchCondition);
        }
        sourceNotMatchedBean.setMatchedBean(visitMerge_matched(ctx.merge_matched()));
        return sourceNotMatchedBean;
    }


}
