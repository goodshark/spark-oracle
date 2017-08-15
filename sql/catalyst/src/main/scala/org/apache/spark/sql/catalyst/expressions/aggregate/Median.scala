
package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.types._

import scala.collection.mutable

/*
  * Created by zhongdg1 on 2017/8/3.
  */
case class Median(child: Expression, mutableAggBufferOffset: Int = 0,
                  inputAggBufferOffset: Int = 0) extends ImperativeAggregate {

  def this(child: Expression) = this(child, 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int):
  ImperativeAggregate = copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int):
  ImperativeAggregate = copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "median"

  private val buffer: mutable.ArrayBuffer[Double] = mutable.ArrayBuffer.empty

  override def initialize(mutableAggBuffer: InternalRow): Unit = {
    buffer.clear()
  }

  override def update(mutableAggBuffer: InternalRow, inputRow: InternalRow): Unit = {
    val value = child.eval(inputRow)

    if (null != value) {
      buffer += {
        value match {
          case v: Long => v.asInstanceOf[Long].toDouble
          case v: Int => v.asInstanceOf[Int].toDouble
          case v: Float => v.asInstanceOf[Float].toDouble
          case v: BigDecimal => v.asInstanceOf[BigDecimal].toDouble
        }
      }
    }
  }

  override def merge(mutableAggBuffer: InternalRow, inputAggBuffer: InternalRow): Unit = {
    sys.error("median cannot be used in partial aggregations.")
  }

  override def eval(input: InternalRow): Any = {
    val sortedBuffer = buffer.sorted
    val len = sortedBuffer.length
    if (len % 2 == 0) {
      (sortedBuffer(len / 2 - 1) + sortedBuffer(len / 2)) / 2
    } else {
      sortedBuffer(len / 2)
    }
  }

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override def aggBufferAttributes: Seq[AttributeReference] = Nil


  override def inputAggBufferAttributes: Seq[AttributeReference] = Nil


  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def nullable: Boolean = true

  override def dataType: DataType = DoubleType


  override def children: Seq[Expression] = child :: Nil

  override def supportsPartial: Boolean = false

}
