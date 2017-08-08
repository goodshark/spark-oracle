package org.apache.hive.plsql.function;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.hive.plsql.PLsqlVisitorImpl;
import org.apache.hive.plsql.PlsqlBaseVisitor;
import org.apache.hive.plsql.PlsqlLexer;
import org.apache.hive.plsql.PlsqlParser;
import org.apache.hive.tsql.common.RootNode;
import org.apache.hive.tsql.ddl.CreateFunctionStatement;
import org.apache.hive.tsql.exception.ParserErrorListener;
import org.apache.spark.sql.catalyst.expressions.*;
import org.apache.spark.sql.types.DataType;
import scala.collection.JavaConversions;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by chenfl2 on 2017/7/10.
 */
public class FunctionCodeGenerator extends PlsqlBaseVisitor<Object> {

    public static void main(String[] args) throws Exception {
//        String sql = "create or replace function func1(n in INTEGER) " +
//                "return INTEGER IS " +
//                "  x INT := 0; " +
//                "  y INT; " +
//                "  z STRING := 22; " +
//                "  u INT := 1; " +
//                "BEGIN " +
//                "  IF n>5 THEN " +
//                "    x := 10; " +
//                "    IF n>10 THEN " +
//                "      y := 5; " +
//                "    ELSIF n>20 THEN " +
//                "      y := 6; " +
//                "    ELSE  " +
//                "      y := 4; " +
//                "    END IF; " +
//                "  ELSE " +
//                "    x := 5; " +
//                "  END IF; " +
//                "CASE u " +
//                "  WHEN 0 THEN x := 1;" +
//                "  WHEN 1 THEN x := 2;" +
//                "  ELSE x := 3;" +
//                "END CASE; " +
//                "CASE " +
//                "  WHEN u=0 THEN x := 1;" +
//                "  WHEN u=1 THEN x := 2;" +
//                "  ELSE x := 3;" +
//                "END CASE; " +
//                "CASE " +
//                "  WHEN u=0 THEN x := 1;" +
//                "  WHEN u=1 THEN x := 2;" +
//                "END CASE; " +
//                "EXCEPTION " +
//                " WHEN CASE_NOT_FOUND THEN " +
//                "   x := 3; " +
//                "WHILE not (x<10 or y>0 and not y>4) LOOP  " +
//                "    x:=x+1;  " +
//                "    y:=y-1; " +
//                "END LOOP;  " +
//                "LOOP " +
//                "  x := x + 1;" +
//                "  IF x>10 THEN " +
//                "    EXIT; " +
//                "  END IF; " +
//                "END LOOP; " +
//                "LOOP " +
//                "  x := x + 1;" +
//                "  EXIT WHEN x>10; " +
//                "END LOOP; " +
//                "<<out>> " +
//                "LOOP " +
//                "  x := x + 1;" +
//                "  EXIT out WHEN x>10; " +
//                "END LOOP out; " +
//                "LOOP " +
//                "  x := x + 1;" +
//                "  LOOP " +
//                "    x := x + 1;" +
//                "    EXIT WHEN x>6; " +
//                "  END LOOP; " +
//                "  EXIT WHEN x>10; " +
//                "END LOOP; " +
//                "LOOP " +
//                "  x := x + 1;" +
//                "  IF x<3 THEN " +
//                "      CONTINUE; " +
//                "  END IF;" +
//                "  EXIT WHEN x>10; " +
//                "END LOOP; " +
//                "LOOP " +
//                "  x := x + 1;" +
//                "  CONTINUE WHEN x<3;  " +
//                "  EXIT WHEN x>10; " +
//                "END LOOP; " +
//                "FOR i IN 1..3 LOOP "+
//                "  x := x + i; "+
//                "END LOOP; " +
//                "FOR i IN REVERSE 1..3 LOOP "+
//                "  x := x + i; "+
//                "END LOOP; " +
//                "<<out1>> " +
//                "FOR i IN REVERSE 1..3 LOOP "+
//                "  x := x + i; "+
//                "END LOOP out1; " +
//                "FOR i IN REVERSE x..u LOOP "+
//                "  x := x + i; "+
//                "  EXIT WHEN x>3; "+
//                "END LOOP; " +
//                "<<gt>> " +
//                "  x := x + 1; " +
//                "  IF x<10 THEN " +
//                "  GOTO gt;" +
//                "  END IF;" +
//                "  RETURN n; " +
//                "END;";
//        System.out.println(FakeFunction.class.getName());
//        System.out.println(FakeFunction.class.getSimpleName());
//        System.out.println(Boolean.class.isAssignableFrom(String.class));
        Murmur3Hash hash = new Murmur3Hash(JavaConversions.asScalaBuffer(Arrays.asList(new Expression[]{new FunctionValueExpression(0)})));
        Murmur3Hash hash2 = new Murmur3Hash(JavaConversions.asScalaBuffer(Arrays.asList(new Expression[]{new FunctionValueExpression(0)})));
        FunctionArgsRow row = new FunctionArgsRow(1);
        FunctionArgsRow row2 = new FunctionArgsRow(1);
//        row2.update(0,10);

//        int v = (Integer)(hash2.eval(row2));
//        System.out.println(v);
//        row.update(0, v);
//        System.out.println(hash.eval(row));
        System.out.println(hash.eval(row.update(new UpdateValue[]{new UpdateValue(0,10)})));
        System.out.println("===============");
        String sql = "create or replace function func1(n in INTEGER) " +
                "return INTEGER IS " +
                "  x INT := 0; " +
                "  y INT := n + 1; " +
                "  z STRING := '22'; " +
                "  u INT := 1; " +
                "BEGIN " +
                " hash('0', 1, 0 + 1, 2 + (3-1), hash(0));" +
                "  RETURN n; " +
                "END;";
        InputStream inputStream = inputStream = new ByteArrayInputStream(sql.getBytes("UTF-8"));
        ANTLRInputStream input = new ANTLRInputStream(inputStream);

        PlsqlLexer lexer = new PlsqlLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        PlsqlParser parser = new PlsqlParser(tokens);
        ParserErrorListener listener = new ParserErrorListener();
        parser.addErrorListener(listener);
        ParseTree tree = parser.compilation_unit();
        RootNode root = new RootNode();
        PLsqlVisitorImpl pLsqlVisitor = new PLsqlVisitorImpl(root);
        pLsqlVisitor.visit(tree);
        System.out.println("=====");
    }

    public static void generate(CreateFunctionStatement cfs){

    }

}

class Test{

}
