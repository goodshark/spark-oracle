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

package org.apache.spark.sql.execution.datasources

import org.antlr.v4.runtime.misc.Interval
import org.antlr.v4.runtime.tree.ParseTree
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.types._

import scala.collection.mutable

case class CreateTable(
                        tableDesc: CatalogTable,
                        mode: SaveMode,
                        query: Option[LogicalPlan]) extends Command {
  assert(tableDesc.provider.isDefined, "The table to be created must have a provider.")

  if (query.isEmpty) {
    assert(
      mode == SaveMode.ErrorIfExists || mode == SaveMode.Ignore,
      "create table without data insertion can only use ErrorIfExists or Ignore as SaveMode.")
  }

  override def innerChildren: Seq[QueryPlan[_]] = query.toSeq
}

/**
  * Create or replace a local/global temporary view with given data source.
  */
case class CreateTempViewUsing(
                                tableIdent: TableIdentifier,
                                userSpecifiedSchema: Option[StructType],
                                replace: Boolean,
                                global: Boolean,
                                provider: String,
                                options: Map[String, String]) extends RunnableCommand {

  if (tableIdent.database.isDefined) {
    throw new AnalysisException(
      s"Temporary view '$tableIdent' should not have specified a database")
  }

  override def argString: String = {
    s"[tableIdent:$tableIdent " +
      userSpecifiedSchema.map(_ + " ").getOrElse("") +
      s"replace:$replace " +
      s"provider:$provider " +
      CatalogUtils.maskCredentials(options)
  }

  def run(sparkSession: SparkSession): Seq[Row] = {
    val dataSource = DataSource(
      sparkSession,
      userSpecifiedSchema = userSpecifiedSchema,
      className = provider,
      options = options)

    val catalog = sparkSession.sessionState.catalog
    val viewDefinition = Dataset.ofRows(
      sparkSession, LogicalRelation(dataSource.resolveRelation())).logicalPlan

    if (global) {
      catalog.createGlobalTempView(tableIdent.table, viewDefinition, replace)
    } else {
      catalog.createTempView(tableIdent.table, viewDefinition, replace)
    }

    Seq.empty[Row]
  }
}

case class RefreshTable(tableIdent: TableIdentifier)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // Refresh the given table's metadata. If this table is cached as an InMemoryRelation,
    // drop the original cached version and make the new version cached lazily.
    sparkSession.catalog.refreshTable(tableIdent.quotedString)
    Seq.empty[Row]
  }
}


case class AcidUpdateCommand(ctx: UpdateContext, tableIdent: TableIdentifier,
                             tableNameAlias: String,
                             tableNames: mutable.HashMap[String, TableIdentifier])
  extends RunnableCommand {

  val OPTION_TYPE: String = "OPTION_TYPE"
  val SPARK_TRANSACTION_ACID: String = "spark.transaction.acid"
  val vid: String = "vid_crud__column_name"

  def extractWhereMap(node: ParseTree, pmap: mutable.HashMap[String, String]): Unit = {
    // var node = statement.where
    if (node.getChildCount > 1) {
      val childCnt = node.getChildCount
      for (i <- 0 until childCnt) {
        extractWhereMap(node.getChild(i), pmap)
      }
    } else if (node.getChildCount == 1) {
      val leafText = node.getChild(0).getText
      if (leafText.contains("=")) {
        pmap += (leafText.split("=")(0) -> leafText.split("=")(1))
      }
    }
  }

  def parseAcidSql(sessionState: SessionState): String = {
    val statement: UpdateStatementContext = ctx.updateStatement()
    // extract where
    val tableName: String = tableIdent.table
    val dbName: Option[String] = tableIdent.database
    val identifier: TableIdentifier = TableIdentifier(tableName,
      dbName)
    val tableMetadata =
      sessionState.catalog.getTableMetadata(identifier)

    val tb: String = sessionState.catalog.getTableName(identifier)
    val db: String = sessionState.catalog.getDbName(identifier)
    if (!sessionState.catalog.checkAcidTable(tableMetadata)) {
      throw new Exception(s" $tableName is not transaction table")
    }
    val sb: StringBuilder = new StringBuilder()
    val colString: StringBuilder = new StringBuilder()
    var partitionSet: Set[String] = Set()

    var columnMap: Map[String, String] = Map()
    for (i <- 0 until statement.assignlist.size()) {
      val array = statement.assignExpression(i).getText.split("=")
      val ex = statement.assignExpression(i).expression()
      val values = ex.start.getInputStream().getText(
        new Interval(ex.start.getStartIndex(), ex.stop.getStopIndex()))
      if (array(0).contains(".")) {
        columnMap += (array(0).split("\\.")(1).toLowerCase -> values)
      } else {
        columnMap += (array(0).toLowerCase -> values)
      }
    }

    /**
      * bucket number =1 时可以更新桶字段
      */
    if (tableMetadata.bucketSpec
      .getOrElse(new BucketSpec(-1, Seq(), Seq())).numBuckets > 1) {
      tableMetadata.bucketSpec
        .getOrElse(new BucketSpec(-1, Seq(), Seq())).bucketColumnNames.foreach(bucketColumnName => {
        if (columnMap.contains(bucketColumnName.toLowerCase())) {
          throw new Exception(s" Cannot update bucketColumnName: ${bucketColumnName}")
        }
      })
    }


    tableMetadata.partitionColumnNames.foreach(p => {
      if (columnMap.contains(p)) {
        throw new Exception(s" Cannot update partitionColumnName: ${p}")
      }
    })


    sb.append(" insert into ")
    sb.append(db)
    sb.append(".")
    sb.append(tb)
    sb.append("(")
    columnMap.keySet.foreach(k => {
      sb.append("`")
      sb.append(k)
      sb.append("`")
      sb.append(",")
    }
    )
    sb.append(vid)
    sb.append(")")
    if (tableMetadata.partitionColumnNames.nonEmpty) {
      if (null == statement.where || statement.where.getChildCount <= 0) {
        throw new Exception(s" transaction table:${tableName} does not support Dynamic partition ")
      }
      // extract where leaf node
      val partitionColumnMap: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
      extractWhereMap(statement.where, partitionColumnMap)
      partitionSet = tableMetadata.partitionColumnNames.map(cata => cata.toLowerCase).toSet
      val verifyPartition = partitionSet.subsetOf(
        partitionColumnMap.map(ele => ele._1).toSet
      )
      if (!verifyPartition) {
        throw new Exception(s" transaction table:${tableName} does not support Dynamic partition ")
      }
      sb.append(" partition ")
      sb.append("( ")
      var partitionColAssign = List[String]()
      tableMetadata.partitionColumnNames.foreach(p => {
        partitionColAssign = partitionColAssign :+ p + "=" + partitionColumnMap.get(p).get
      })
      sb.append(partitionColAssign.mkString(","))
      sb.append(" )")
    }

    sb.append(" select ")


    columnMap.keySet.foreach(k => {
      sb.append(columnMap.get(k).get)
      sb.append(",")
    }
    )

    /* tableMetadata.schema.foreach(column => {
       if (columnMap.contains(column.name.toLowerCase)) {
         sb.append(columnMap.get(column.name.toLowerCase).get)
         sb.append(",")
       } */
    /* else {
      if (!partitionSet.contains(column.name)) {
        if (null == tableNameAlias || tableNameAlias.equalsIgnoreCase(db + "." + tb)) {
          colString.append(tb)
        } else {
          colString.append(tableNameAlias)
        }
        colString.append(".")
        colString.append(column.name.toLowerCase)
        colString.append(",")
      }
    }
     }) */
    if (null == tableNameAlias || tableNameAlias.equalsIgnoreCase(db + "." + tb)) {
      sb.append(tb + "." + vid + " ")
    } else {
      sb.append(tableNameAlias + "." + vid + " ")
    }
    sb.append(" from ")
    if (null != statement.fromClauseForUpdate()) {
      val fromClause = statement.fromClauseForUpdate()
      sb.append(fromClause.start.getInputStream().getText(
        new Interval(fromClause.start.getStartIndex(), fromClause.stop.getStopIndex())))
      // delete from t11 from t12 where t11.id = t12.id
      // 这样的情况需要在from 后面再追加t11表
      appendUpdateTable(fromClause, sb, db + "." + tb, sessionState.catalog.getCurrentDatabase)


    } else {
      sb.append(db)
      sb.append(".")
      sb.append(tb)
    }
    if (null != statement.where && statement.where.getChildCount > 0) {
      /* sb.append("  where ")
      extractWhereStr(statement.where, sb) */
      sb.append(" where ")
      val where = statement.where
      sb.append(where.start.getInputStream().getText(
        new Interval(where.start.getStartIndex(), where.stop.getStopIndex())))
    }

    if (ctx.updateStatement().LIMIT() != null) {
      sb.append(" limit ")
      sb.append(ctx.updateStatement().expression().getText)
    }

    sessionState.conf.setConfString(OPTION_TYPE, "1")
    sessionState.conf.setConfString(SPARK_TRANSACTION_ACID, "true")
    // sessionState.conf.setConfString("spark.acid.update.value", updateValues.toString())
    logInfo(s" paser  update sql ====> " + sb.toString())
    sb.toString()
  }

  def appendUpdateTable(fromClause: FromClauseForUpdateContext,
                        sb: StringBuilder, updateTable: String, currentDb: String): Unit = {
    val relations = fromClause.relationUpate()
    var tableSet: Set[String] = Set()
    for (i <- 0 until (relations.size())) {
      val table = relations.get(i).tableNameUpdate().tableIdentifier()
      if (table.identifier().size() == 2) {
        tableSet += (table.identifier(0).getText.toLowerCase
          + "." + table.identifier(1).getText.toLowerCase)
      } else {
        tableSet += (currentDb + "." + table.identifier(0).getText.toLowerCase)
      }
    }
    logWarning(s"fromClusss table set is => ${tableSet}")
    if (!tableSet.contains(updateTable)) {
      sb.append(" ,")
      sb.append(updateTable)
      sb.append(" ")
    }

  }


  override def run(sparkSession: SparkSession): Seq[Row] = {
    Seq.empty[Row]
  }
}

case class AcidDelCommand(ctx: DeleteContext, tableIdentifier: TableIdentifier,
                          tableNameAlias: String)
  extends RunnableCommand {

  def getInsertColumnName(tableMetadata: CatalogTable, partitionSet: Set[String]): String = {
    tableMetadata.schema.foreach(c => {
      if (!partitionSet.contains(c.name)) {
        return c.name
      }
    })
    return ""
  }

  def parseAcidSql(sessionState: SessionState): String = {
    val statement: DeleteStatementContext = ctx.deleteStatement()

    val tableName: String = tableIdentifier.table
    val dbName: Option[String] = tableIdentifier.database
    val identifier: TableIdentifier = TableIdentifier(tableName,
      dbName)
    val tableMetadata =
      sessionState.catalog.getTableMetadata(identifier)
    val tb: String = sessionState.catalog.getTableName(identifier)
    val db: String = sessionState.catalog.getDbName(identifier)
    if (!sessionState.catalog.checkAcidTable(tableMetadata)) {
      throw new Exception(s" $tableName is not transaction table")
    }
    val sb: StringBuilder = new StringBuilder()
    val colString: StringBuilder = new StringBuilder()
    sb.append(" insert into ")
    sb.append(db)
    sb.append(".")
    sb.append(tb)



    var partitionSet: Set[String] = Set()
    if (tableMetadata.partitionColumnNames.nonEmpty) {
      partitionSet = tableMetadata.partitionColumnNames.map(cata => cata.toLowerCase).toSet
    }
    sb.append("(")
    sb.append("`")
    val insertColName = getInsertColumnName(tableMetadata, partitionSet)
    sb.append(insertColName)
    sb.append("`")
    sb.append("," + AcidUpdateCommand(null, identifier, null, null).vid)
    sb.append(")")

    if (tableMetadata.partitionColumnNames.nonEmpty) {
      if (null == statement.where || statement.where.getChildCount <= 0) {
        throw new Exception(s" transaction table:${tableName} does not support Dynamic partition ")
      }
      // extract where leaf node
      val partitionColumnMap: mutable.HashMap[String, String] = new mutable.HashMap[String, String]()
      AcidUpdateCommand(null, identifier, null, null)
        .extractWhereMap(statement.where, partitionColumnMap)
      val verifyPartition = partitionSet.subsetOf(
        partitionColumnMap.map(ele => ele._1).toSet
      )
      if (!verifyPartition) {
        throw new Exception(s" transaction table:${tableName} does not support Dynamic partition ")
      }
      sb.append(" partition ")
      sb.append("( ")
      var partitionColAssign = List[String]()
      tableMetadata.partitionColumnNames.foreach(p => {
        partitionColAssign = partitionColAssign :+ p + "=" + partitionColumnMap.get(p).get
      })
      sb.append(partitionColAssign.mkString(","))
      sb.append(" )")
    }

    sb.append(" select NULL , ")
   /* tableMetadata.schema.foreach(c => {
      if (!partitionSet.contains(c.name)) {
        colString.append("NULL")
        colString.append(",")
      }
    })
    sb.append(colString.toString())
    sb.append(" ") */
    if (null == tableNameAlias || tableNameAlias.equalsIgnoreCase(db + "." + tb)) {
      sb.append(tb + "." + AcidUpdateCommand(null, identifier, null, null).vid).append(" ")
    } else {
      sb.append(tableNameAlias + "."
        + AcidUpdateCommand(null, identifier, null, null).vid).append(" ")
    }

    if (statement.joinRelationUpate().isEmpty) {
      if (null != statement.fromTable()) {
        val fromTable = statement.fromTable
        sb.append(fromTable.start.getInputStream().getText(
          new Interval(fromTable.start.getStartIndex(), fromTable.stop.getStopIndex()))
          .replaceAll("\\(", ""))
        sb.append(", ")
        sb.append(db)
        sb.append(".")
        sb.append(tb)
        sb.append(" ")
        sb.append(tableNameAlias)
        sb.append(" ")
      } else {
        sb.append("  from ")
        sb.append(db)
        sb.append(".")
        sb.append(tb)
        sb.append(" ")
      }

    } else {
      if (null != statement.fromTable()) {
        val fromTable = statement.fromTable
        sb.append(fromTable.start.getInputStream().getText(
          new Interval(fromTable.start.getStartIndex(), fromTable.stop.getStopIndex()))
          .replaceAll("\\(", ""))
          .append(" ")
      }

      val joinRelation = statement.joinRelationUpate()
      val iterator = joinRelation.iterator()
      while (iterator.hasNext) {
        val j = iterator.next()
        sb.append(j.start.getInputStream().getText(
          new Interval(j.start.getStartIndex(), j.stop.getStopIndex()))).append(" ")
      }

    }
    if (null != statement.where && statement.where.getChildCount > 0) {
      sb.append(" where ")
      val where = statement.where
      sb.append(where.start.getInputStream().getText(
        new Interval(where.start.getStartIndex(), where.stop.getStopIndex())))
    }


    if (ctx.deleteStatement().LIMIT() != null) {
      sb.append(" limit ")
      sb.append(ctx.deleteStatement().expression().getText)
    }
    sessionState.conf.setConfString(AcidUpdateCommand(null,
      identifier, null, null).OPTION_TYPE, "2")
    sessionState.conf.setConfString(
      AcidUpdateCommand(null, identifier, null, null).SPARK_TRANSACTION_ACID,
      "true")
    logInfo(s" parse del sql ====> " + sb.toString())
    sb.toString()
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    Seq.empty[Row]
  }
}

case class RefreshResource(path: String)
  extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    sparkSession.catalog.refreshByPath(path)
    Seq.empty[Row]
  }
}

/**
  * Builds a map in which keys are case insensitive
  */
class CaseInsensitiveMap(map: Map[String, String]) extends Map[String, String]
  with Serializable {

  val baseMap = map.map(kv => kv.copy(_1 = kv._1.toLowerCase))

  override def get(k: String): Option[String] = baseMap.get(k.toLowerCase)

  override def +[B1 >: String](kv: (String, B1)): Map[String, B1] =
    baseMap + kv.copy(_1 = kv._1.toLowerCase)

  override def iterator: Iterator[(String, String)] = baseMap.iterator

  override def -(key: String): Map[String, String] = baseMap - key.toLowerCase
}
