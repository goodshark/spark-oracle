/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.generic.Growable
import scala.collection.mutable

/**
  * The Collect aggregate function collects all seen expression values into a list of values.
  *
  * The operator is bound to the slower sort based aggregation path because the number of
  * elements (and their memory usage) can not be determined in advance. This also means that the
  * collected elements are stored on heap, and that too many elements can cause GC pauses and
  * eventually Out of Memory Errors.
  */
abstract class Collect extends ImperativeAggregate {

  val child: Expression

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  override def dataType: DataType = ArrayType(child.dataType)

  override def inputTypes: Seq[AbstractDataType] = Seq(AnyDataType)

  override def supportsPartial: Boolean = false

  override def aggBufferAttributes: Seq[AttributeReference] = Nil

  override def aggBufferSchema: StructType = StructType.fromAttributes(aggBufferAttributes)

  override def inputAggBufferAttributes: Seq[AttributeReference] = Nil

  // Both `CollectList` and `CollectSet` are non-deterministic since their results depend on the
  // actual order of input rows.
  override def deterministic: Boolean = false

  protected[this] val buffer: Growable[Any] with Iterable[Any]

  override def initialize(b: InternalRow): Unit = {
    buffer.clear()
  }

  override def update(b: InternalRow, input: InternalRow): Unit = {
    // Do not allow null values. We follow the semantics of Hive's collect_list/collect_set here.
    // See: org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMkCollectionEvaluator
    val value = child.eval(input)
    if (value != null) {
      buffer += value
    }
  }

  override def merge(buffer: InternalRow, input: InternalRow): Unit = {
    sys.error("Collect cannot be used in partial aggregations.")
  }

  override def eval(input: InternalRow): Any = {
    new GenericArrayData(buffer.toArray)
  }
}

/**
  * Collect a list of elements.
  */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Collects and returns a list of non-unique elements.")
case class CollectList(
                        child: Expression,
                        mutableAggBufferOffset: Int = 0,
                        inputAggBufferOffset: Int = 0) extends Collect {

  def this(child: Expression) = this(child, 0, 0)

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "collect_list"

  override protected[this] val buffer: mutable.ArrayBuffer[Any] = mutable.ArrayBuffer.empty
}

/**
  * Collect a set of unique elements.
  */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Collects and returns a set of unique elements.")
case class CollectSet(
                       child: Expression,
                       mutableAggBufferOffset: Int = 0,
                       inputAggBufferOffset: Int = 0) extends Collect {

  def this(child: Expression) = this(child, 0, 0)

  override def checkInputDataTypes(): TypeCheckResult = {
    if (!child.dataType.existsRecursively(_.isInstanceOf[MapType])) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure("collect_set() cannot have map type data")
    }
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "collect_set"

  override protected[this] val buffer: mutable.HashSet[Any] = mutable.HashSet.empty
}

/**
  * Concat a list of elements.in a group.
  */
@ExpressionDescription(
  usage = "_FUNC_(expr) - Concat a list of elements.in a group.")
case class CollectGroupXMLPath(
                                cols: Seq[Expression],
                                mutableAggBufferOffset: Int = 0,
                                inputAggBufferOffset: Int = 0) extends Collect {


  def this(cols: Seq[Expression]) = this(cols, 0, 0)

  override val child = null

  override def children: Seq[Expression] = cols

  override def nullable: Boolean = true

  override def dataType: DataType = StringType

  override def inputTypes: Seq[AbstractDataType] = Seq.fill(children.size)(AnyDataType)


  override def aggBufferAttributes: Seq[AttributeReference] = super.aggBufferAttributes

  override def checkInputDataTypes(): TypeCheckResult = {
    val allOK = cols.forall(child =>
      !child.dataType.existsRecursively(_.isInstanceOf[MapType]))
    if (allOK) {
      TypeCheckResult.TypeCheckSuccess
    } else {
      TypeCheckResult.TypeCheckFailure("group_xmlpath() cannot have map type data")
    }
  }

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def prettyName: String = "group_xmlpath"

  override protected[this] val buffer: mutable.ArrayBuffer[Any] = mutable.ArrayBuffer.empty

  protected[this] val strbuffer: StringBuilder = new StringBuilder

  private var columnNames: Seq[(String, String)] = Seq.fill(cols.length)(("", ""))

  private var rootSpec = ("<root>", "</root>")
  private var rowSpec = ("<row>", "</row>")


  override def initialize(b: InternalRow): Unit = {
    buffer.clear()

    strbuffer.clear()

    initializeColNames

    strbuffer.append(rootSpec._1)
  }
  private def initializeColNames = {
    cols.last match {
      case Literal(v, d)  if d.isInstanceOf[ArrayType] =>
        val av = v.asInstanceOf[GenericArrayData]
        val names = av.array.map( _.toString.trim )
        val namepair = names.map(e => if ( e.length > 0 ) (s"<$e>", s"</$e>") else ("", "")).toSeq
        rootSpec = namepair(0)
        rowSpec = namepair(1)
        columnNames = namepair.slice(2, namepair.length)
      case _ =>
    }
  }

  override def update(b: InternalRow, input: InternalRow): Unit = {
    strbuffer.append(rowSpec._1)
    for( i <- 0 to ( cols.length - 2) ) {
      strbuffer.append(columnNames(i)._1)
        .append(cols(i).eval(input))
        .append(columnNames(i)._2)
    }
    strbuffer.append(rowSpec._2)
  }

  override def merge(buffer: InternalRow, input: InternalRow): Unit = {
    sys.error("group_xmlpath cannot be used in partial aggregations.")
  }

  override def eval(input: InternalRow): Any = {
    strbuffer.append(rootSpec._2)
    UTF8String.fromString(strbuffer.toString())
  }
}