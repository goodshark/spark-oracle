/*
  * Created by chenfolin on 2017/7/27.
  */
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types.{AbstractDataType, DataType, StringType}

case class PlFunction(children: Seq[Expression], funcname: String)
      extends Expression with ImplicitCastInputTypes{

  override def inputTypes: Seq[AbstractDataType] = children.map(c => c.dataType)
  override def dataType: DataType = StringType

  override def nullable: Boolean = children.exists(_.nullable)
  override def foldable: Boolean = children.forall(_.foldable)

  override def eval(input: InternalRow): Any = {
    funcname
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val evals = children.map(_.genCode(ctx))
    val inputs = evals.map { eval =>
      s"${eval.isNull} ? null : ${eval.value}"
    }.mkString(", ")
    ev.copy(evals.map(_.code).mkString("\n") + s"""
      boolean ${ev.isNull} = false;
      UTF8String ${ev.value} = UTF8String.fromString("$funcname");
      if (${ev.value} == null) {
        ${ev.isNull} = true;
      }
    """)
  }

}
