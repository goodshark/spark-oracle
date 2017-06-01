/*
 * Created by zhongdg1 on 2017/5/17.
 */
package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, ForClauseDetail, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.types.{DataType, StringType}
import org.apache.spark.unsafe.types.UTF8String

case class ForClauseExec(forClauseDetail: ForClauseDetail, output: Seq[Attribute]
                         , child: SparkPlan, xmlElemNames: Seq[String])
  extends UnaryExecNode {

  private val serializer: Serializer = new UnsafeRowSerializer(1)

  def hasRowLabel(): Boolean = {
    forClauseDetail.forXmlClause.rowLabel.length > 0
  }

  /**
    * Overridden by concrete implementations of SparkPlan.
    * Produces the result of the query as an RDD[InternalRow]
    */
  override protected def doExecute(): RDD[InternalRow] = {

    val childRdd = child.execute()

    val shuffled = new ShuffledRowRDD(
      ShuffleExchange.prepareShuffleDependency(
        childRdd, child.output, SinglePartition, serializer))
    shuffled.mapPartitionsInternal(iter => {
      val converter = UnsafeProjection.create(Array[DataType](StringType))
      val builder = new StringBuilder
      val columnLen = child.output.length
      if (hasRoot()) {
        builder.append("<root>")
      }

      while (iter.hasNext) {
        val row = iter.next()
        if (hasRowLabel()) {
          builder.append("<").append(forClauseDetail.forXmlClause.rowLabel).append(">")
        }
        for (i <- 0 to (columnLen - 1)) {
          val value = row.get(i, child.output(i).dataType)
          var hasColumnLabel = true
          if (null == xmlElemNames(i) || xmlElemNames(i).length == 0) {
            hasColumnLabel = false
          }
          if (hasColumnLabel) {
            builder.append("<").append(xmlElemNames(i)).append(">")
          }

          builder.append(value)
          if (hasColumnLabel) {
            builder.append("</").append(xmlElemNames(i)).append(">")
          }
        }

        if (hasRowLabel()) {
          builder.append("</").append(forClauseDetail.forXmlClause.rowLabel).append(">")
        }
      }

      if (hasRoot()) {
        builder.append("</root>")
      }

      Iterator(converter.apply(InternalRow(UTF8String.fromString(builder.toString()))))
    })
  }

  def hasRoot(): Boolean = {
    forClauseDetail.forXmlClause.hasRoot
  }

  /** Specifies how data is partitioned across different nodes in the cluster. */
  override def outputPartitioning: Partitioning = SinglePartition

  override def simpleString: String = {
    s"($forClauseDetail, projects=$output, xmlElemNames=$xmlElemNames)"
  }
}
