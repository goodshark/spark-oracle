
package org.apache.spark.sql.catalyst.expressions.xml

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.aggregate.ImperativeAggregate
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BinaryExpression, Expression, ImplicitCastInputTypes, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable

/*
 * Created by zhongdg1 on 2017/8/8.
 */
case class XmlElement(left: Expression, right: Expression) extends BinaryExpression {

//  private lazy val columnName = left.eval()

  /**
    * Returns Java source code that can be compiled to evaluate this expression.
    * The default behavior is to call the eval method of the expression. Concrete expression
    * implementations should override this to do actual code generation.
    *
    * @param ctx a [[CodegenContext]]
    * @param ev  an [[ExprCode]] with unique terms.
    * @return an [[ExprCode]] containing the Java source code to generate the given expression
    */
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    nullSafeCodeGen(ctx, ev, (columnName, v) => {
      s"""
         |${ev.value} = UTF8String.fromString(new StringBuffer().
         |append("<").append($columnName).append(">")
         |.append($v).append("</").append($columnName).append(">").toString());
      """.stripMargin
    })
  }

  /**
    * Returns the [[DataType]] of the result of evaluating this expression.  It is
    * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
    */
  override def dataType: DataType = StringType

  override def foldable: Boolean = false

  override def nullable: Boolean = true

  /**
    * Returns a user-facing string representation of this expression's name.
    * This should usually match the name of the function in SQL.
    */
  override def prettyName: String = "xmlelement"

  override def simpleString: String = "xmlelement"
}


case class XmlAgg(child: Expression, mutableAggBufferOffset: Int = 0,
                  inputAggBufferOffset: Int = 0) extends ImperativeAggregate {

  def this(child: Expression) = this(child, 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int):
  ImperativeAggregate = copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int):
  ImperativeAggregate = copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "xmlagg"

  private val buffer: mutable.ArrayBuffer[String] = mutable.ArrayBuffer.empty

  override def initialize(mutableAggBuffer: InternalRow): Unit = {
    buffer.clear()
  }

  override def update(mutableAggBuffer: InternalRow, inputRow: InternalRow): Unit = {
    val value = child.eval(inputRow)

    if (null != value) {
      buffer += {
        value.toString
      }
    }
  }

  override def merge(mutableAggBuffer: InternalRow, inputAggBuffer: InternalRow): Unit = {
    sys.error("median cannot be used in partial aggregations.")
  }

  override def eval(input: InternalRow): Any = {
    UTF8String.fromString(buffer.mkString(""))
  }

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override def aggBufferAttributes: Seq[AttributeReference] = Nil


  override def inputAggBufferAttributes: Seq[AttributeReference] = Nil


  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def nullable: Boolean = true

  override def dataType: DataType = StringType


  override def children: Seq[Expression] = child :: Nil

  override def supportsPartial: Boolean = false

}

case class XmlColattval(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq.fill(children.size)(StringType)
  override def dataType: DataType = StringType

  override def nullable: Boolean = children.exists(_.nullable)
  override def foldable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val inputs = children.map(_.eval(input).asInstanceOf[UTF8String])
    UTF8String.concat(inputs : _*)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val mid = children.length / 2
    val evals = children.take(mid).map(_.genCode(ctx))
    val inputs = evals.map { eval =>
      s"${eval.isNull} ? null : ${eval.value}"
    }.mkString(", ")

    val cols = "\"" + children.takeRight(mid).map(_.eval()).mkString("#") + "\""

    val t = ev.copy(evals.map(_.code).mkString("\n") + s"""
      boolean ${ev.isNull} = false;
      UTF8String ${ev.value} = UTF8String.xmlColattval($cols, $inputs);
      if (${ev.value} == null) {
        ${ev.isNull} = true;
      }
    """)

    t
  }
}


case class XmlForest(children: Seq[Expression])
  extends Expression with ImplicitCastInputTypes {

  override def inputTypes: Seq[AbstractDataType] = Seq.fill(children.size)(StringType)
  override def dataType: DataType = StringType

  override def nullable: Boolean = children.exists(_.nullable)
  override def foldable: Boolean = false

  override def eval(input: InternalRow): Any = {
    val inputs = children.map(_.eval(input).asInstanceOf[UTF8String])
    UTF8String.concat(inputs : _*)
  }

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val mid = children.length / 2
    val evals = children.take(mid).map(_.genCode(ctx))
    val inputs = evals.map { eval =>
      s"${eval.isNull} ? null : ${eval.value}"
    }.mkString(", ")

    val cols = "\"" + children.takeRight(mid).map(_.eval()).mkString("#") + "\""

    val t = ev.copy(evals.map(_.code).mkString("\n") + s"""
      boolean ${ev.isNull} = false;
      UTF8String ${ev.value} = UTF8String.xmlForest($cols, $inputs);
      if (${ev.value} == null) {
        ${ev.isNull} = true;
      }
    """)

    t
  }
}


