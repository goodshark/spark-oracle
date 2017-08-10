/*
  * Created by chenfolin on 2017/7/27.
  */
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._

case class PlFunction(children: Seq[Expression], database: String, funcname: String,
                      funccode: String, returnType: String)
      extends Expression with ImplicitCastInputTypes{

  lazy val plFunctionExecutor: PlFunctionExecutor = PlFunction.generateInstance(funccode);

  override def inputTypes: Seq[AbstractDataType] = children.map(c => c.dataType)
  override def dataType: DataType = {
    returnType.toUpperCase match {
      case "STRING" => StringType
      case "CHAR" => StringType
      case "VARCHAR" => StringType
      case "VARCHAR2" => StringType
      case "LONG" => LongType
      case "DOUBLE" => DoubleType
      case "FLOAT" => FloatType
      case "INT" => IntegerType
      case "INTEGER" => IntegerType
      case "BOOLEAN" => BooleanType
      case _ => StringType
    }
  }

  override def nullable: Boolean = children.exists(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  override def eval(input: InternalRow): Any = {
    val inputs = children.map(_.eval(input).asInstanceOf[Object]).toArray
    plFunctionExecutor.eval(inputs)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    PlFunctionInstanceGenerator.addCode(database + "_" + funcname, funccode)
    val evals = children.map(_.genCode(ctx))
    val inputs = evals.map { eval =>
      s"${eval.isNull} ? null : ((Object)${eval.value})"
    }.mkString(", ")
    ctx.addMutableState("org.apache.spark.sql.catalyst.expressions.PlFunctionInstanceGenerator",
      database + "_" + funcname, database + "_" + funcname +
      "= new org.apache.spark.sql.catalyst.expressions.PlFunctionInstanceGenerator();")
    ev.copy(evals.map(_.code).mkString("\n") + s"""
      boolean ${ev.isNull} = false;
      ${returnType} ${ev.value} = (${returnType})((${funcname}${ev.value}."""
       + s"""getFuncInstance("${database}_${funcname}")).eval(new Object[]{${inputs}}));
      if (${ev.value} == null) {
        ${ev.isNull} = true;
      }
    """)
  }

}

object PlFunction {
  def generateInstance(code: String): PlFunctionExecutor = {
    val cleanedSource = CodeFormatter.stripOverlappingComments(
      new CodeAndComment(CodeFormatter.stripExtraNewLines(code), collection.Map.empty))
    val clazz = CodeGenerator.compile(cleanedSource)
    val buffer = clazz.generate(null).asInstanceOf[PlFunctionExecutor]
    buffer
  }
}
