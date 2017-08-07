/*
  * Created by chenfolin on 2017/7/24.
  */
package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._

case class FunctionValueExpression(index: Int)
    extends UnaryExpression {

  val child: Expression = null

  override def dataType: DataType = null

  override def eval(input: InternalRow): Any = {
    if (input.isInstanceOf[FunctionArgsRow]) {
      input.asInstanceOf[FunctionArgsRow].get(index)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    null
  }
}
