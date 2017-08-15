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

package org.apache.spark.sql.hive.thriftserver

import java.security.PrivilegedExceptionAction
import java.sql.{Date, Timestamp}
import java.util
import java.util.{Arrays, UUID, Map => JMap}
import java.util.concurrent.RejectedExecutionException

import org.apache.commons.lang.StringUtils

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, Map => SMap}
import scala.util.control.NonFatal
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.shims.Utils
import org.apache.hive.service.cli._
import org.apache.hive.service.cli.operation.ExecuteStatementOperation
import org.apache.hive.service.cli.session.HiveSession
import org.apache.hive.tsql.common.SparkResultSet
import org.apache.hive.tsql.{ExecSession, ProcedureCli}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.{DataFrame, SQLContext, Row => SparkRow}
import org.apache.spark.sql.execution.command.SetCommand
import org.apache.spark.sql.execution.datasources.{AcidDelCommand, AcidUpdateCommand}
import org.apache.spark.sql.hive.HiveUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, ShortType, StringType, StructType, TimestampType, _}
import org.apache.spark.util.{Utils => SparkUtils}

private[hive] class SparkExecuteStatementOperation(
                                                    parentSession: HiveSession,
                                                    statement: String,
                                                    confOverlay: JMap[String, String],
                                                    runInBackground: Boolean = true)
                                                  (sqlContext: SQLContext, sessionToActivePool: JMap[SessionHandle, String])
  extends ExecuteStatementOperation(parentSession, statement, confOverlay, runInBackground)
    with Logging {

  private var result: DataFrame = _
  private var iter: Iterator[SparkRow] = _
  private var iterHeader: Iterator[SparkRow] = _
  private var dataTypes: Array[DataType] = _
  private var statementId: String = _

  private lazy val resultSchema: TableSchema = {
    if (result == null || result.schema.isEmpty) {
      new TableSchema(Arrays.asList(new FieldSchema("Result", "string", "")))
    } else {
      logInfo(s"Result Schema: ${result.schema}")
      SparkExecuteStatementOperation.getTableSchema(result.schema)
    }
  }

  def close(): Unit = {
    // RDDs will be cleaned automatically upon garbage collection.
    sqlContext.sparkContext.clearJobGroup()
    logDebug(s"CLOSING $statementId")
    cleanup(OperationState.CLOSED)
  }

  def addNonNullColumnValue(from: SparkRow, to: ArrayBuffer[Any], ordinal: Int) {
    dataTypes(ordinal) match {
      case StringType =>
        to += from.getString(ordinal)
      case IntegerType =>
        to += from.getInt(ordinal)
      case BooleanType =>
        to += from.getBoolean(ordinal)
      case DoubleType =>
        to += from.getDouble(ordinal)
      case FloatType =>
        to += from.getFloat(ordinal)
      case DecimalType() =>
        to += from.getDecimal(ordinal)
      case LongType =>
        to += from.getLong(ordinal)
      case ByteType =>
        to += from.getByte(ordinal)
      case ShortType =>
        to += from.getShort(ordinal)
      case DateType =>
        to += from.getAs[Date](ordinal)
      case TimestampType =>
        to += from.getAs[Timestamp](ordinal)
      case BinaryType =>
        to += from.getAs[Array[Byte]](ordinal)
      case _: ArrayType | _: StructType | _: MapType =>
        val hiveString = HiveUtils.toHiveString((from.get(ordinal), dataTypes(ordinal)))
        to += hiveString
    }
  }

  def getNextRowSet(order: FetchOrientation, maxRowsL: Long): RowSet = {
    validateDefaultFetchOrientation(order)
    assertState(OperationState.FINISHED)
    setHasResultSet(true)
    val resultRowSet: RowSet = RowSetFactory.create(getResultSetSchema, getProtocolVersion)

    // Reset iter to header when fetching start from first row
    if (order.equals(FetchOrientation.FETCH_FIRST)) {
      val (ita, itb) = iterHeader.duplicate
      iter = ita
      iterHeader = itb
    }

    if (!iter.hasNext) {
      resultRowSet
    } else {
      // maxRowsL here typically maps to java.sql.Statement.getFetchSize, which is an int
      val maxRows = maxRowsL.toInt
      var curRow = 0
      while (curRow < maxRows && iter.hasNext) {
        val sparkRow = iter.next()
        val row = ArrayBuffer[Any]()
        var curCol = 0
        while (curCol < sparkRow.length) {
          if (sparkRow.isNullAt(curCol)) {
            row += null
          } else {
            addNonNullColumnValue(sparkRow, row, curCol)
          }
          curCol += 1
        }
        resultRowSet.addRow(row.toArray.asInstanceOf[Array[Object]])
        curRow += 1
      }
      resultRowSet
    }
  }

  def getResultSetSchema: TableSchema = resultSchema

  override def runInternal(): Unit = {
    setState(OperationState.PENDING)
    setHasResultSet(true) // avoid no resultset for async run

    if (!runInBackground) {
      execute()
    } else {
      val sparkServiceUGI = Utils.getUGI()

      // Runnable impl to call runInternal asynchronously,
      // from a different thread
      val backgroundOperation = new Runnable() {

        override def run(): Unit = {
          val doAsAction = new PrivilegedExceptionAction[Unit]() {
            override def run(): Unit = {
              try {
                execute()
              } catch {
                case e: HiveSQLException =>
                  setOperationException(e)
                  log.error("Error running hive query: ", e)
              }
            }
          }

          try {
            sparkServiceUGI.doAs(doAsAction)
          } catch {
            case e: Exception =>
              setOperationException(new HiveSQLException(e))
              logError("Error running hive query as user : " +
                sparkServiceUGI.getShortUserName(), e)
          }
        }
      }
      try {
        // This submit blocks if no background threads are available to run this operation
        val backgroundHandle =
          parentSession.getSessionManager().submitBackgroundOperation(backgroundOperation)
        setBackgroundHandle(backgroundHandle)
      } catch {
        case rejected: RejectedExecutionException =>
          setState(OperationState.ERROR)
          throw new HiveSQLException("The background threadpool cannot accept" +
            " new task for execution, please retry the operation", rejected)
        case NonFatal(e) =>
          logError(s"Error executing query in background", e)
          setState(OperationState.ERROR)
          throw e
      }
    }
  }

  private def execute(): Unit = {
    statementId = UUID.randomUUID().toString
    logInfo(s"Running query '$statement' with $statementId")
    setState(OperationState.RUNNING)
    // Always use the latest class loader provided by executionHive's state.
    val executionHiveClassLoader = sqlContext.sharedState.jarClassLoader
    Thread.currentThread().setContextClassLoader(executionHiveClassLoader)

    HiveThriftServer2.listener.onStatementStart(
      statementId,
      parentSession.getSessionHandle.getSessionId.toString,
      statement,
      statementId,
      parentSession.getUsername)
    sqlContext.sparkContext.setJobGroup(statementId, statement)
    val pool = sessionToActivePool.get(parentSession.getSessionHandle)
    if (pool != null) {
      sqlContext.sparkContext.setLocalProperty("spark.scheduler.pool", pool)
    }
    var plan: LogicalPlan = null
    var sqlServerPlans: java.util.List[LogicalPlan] = new util.ArrayList[LogicalPlan]()
    val SQL_ENGINE = "spark.sql.analytical.engine"
    val engineName = sqlContext.sessionState.
      conf.getConfString(SQL_ENGINE, "spark")
    val checkSparkEngineName = engineName.equalsIgnoreCase("spark")
    try {
      if (!checkSparkEngineName) {
        val procCli: ProcedureCli = new ProcedureCli(sqlContext.sparkSession)
        procCli.callProcedure(statement, engineName)
        val sqlServerRs = procCli.getExecSession().getResultSets()
        sqlServerPlans = procCli.getExecSession.getLogicalPlans
        if (null == sqlServerRs || sqlServerRs.size() == 0) {
          import sqlContext.implicits._
          result = sqlContext.sparkSession.createDataset(Seq[String]()).toDF()
        } else {
          result = sqlServerRs.get(sqlServerRs.size() - 1).asInstanceOf[SparkResultSet].getDataset
          // logInfo("sqlServer result is ==>" + result.queryExecution.toString())
        }
        val allTable = new util.HashSet[String]()
        val tmpTable = sqlContext.sparkSession.getTables(2)
        val globalTable = sqlContext.sparkSession.getTables(3)
        if (null != tmpTable) {
          allTable.addAll(tmpTable)
        }
        if (null != globalTable) {
          allTable.addAll(globalTable)
        }
        HiveThriftServer2.sqlSessionListenr.addTable(
          parentSession.getSessionHandle.getSessionId.toString,
          allTable)
      } else {
        plan = sqlContext.sessionState.sqlParser.parsePlan(statement)
        result = sqlContext.sql(statement)
        logDebug(result.queryExecution.toString())
        result.queryExecution.logical match {
          case SetCommand(Some((SQLConf.THRIFTSERVER_POOL.key, Some(value)))) =>
            sessionToActivePool.put(parentSession.getSessionHandle, value)
            logInfo(s"Setting spark.scheduler.pool=$value for future statements in this session.")
          case _ =>
        }
      }

      logInfo("logical is " + result.queryExecution.logical)
      result.queryExecution.logical match {
        case SetCommand(Some((SQL_ENGINE, Some(value)))) =>
          sessionToActivePool.put(parentSession.getSessionHandle, value)
          sqlContext.sessionState.conf.setConfString(SQL_ENGINE, value)
          logInfo(s"Setting spark.sql.analytical.engine=$value " +
            s"for future statements in this session.")
        case _ =>
      }


      HiveThriftServer2.listener.onStatementParsed(statementId, result.queryExecution.toString())
      iter = {
        val useIncrementalCollect =
          sqlContext.getConf("spark.sql.thriftServer.incrementalCollect", "false").toBoolean
        if (useIncrementalCollect) {
          result.toLocalIterator.asScala
        } else {
          result.collect().iterator
        }
      }
      val (itra, itrb) = iter.duplicate
      iterHeader = itra
      iter = itrb
      dataTypes = result.queryExecution.analyzed.output.map(_.dataType).toArray
    } catch {
      case e: HiveSQLException =>
        if (getStatus().getState() == OperationState.CANCELED) {
          return
        } else {
          setState(OperationState.ERROR)
          throw e
        }
      // Actually do need to catch Throwable as some failures don't inherit from Exception and
      // HiveServer will silently swallow them.
      case e: Throwable =>
        val currentState = getStatus().getState()
        logError(s"Error executing query, currentState $currentState, ", e)
        setState(OperationState.ERROR)
        HiveThriftServer2.listener.onStatementError(
          statementId, e.getMessage, SparkUtils.exceptionString(e))
        throw new HiveSQLException(e.toString)
    } finally {
      clearCrudTableMap(plan)
      if (!checkSparkEngineName) {
        clearCrudTableMapForSqlServer(sqlServerPlans)
        dropSqlserverTables
      } else {
        clearCrudTableMap(plan)
      }

    }
    setState(OperationState.FINISHED)
    HiveThriftServer2.listener.onStatementFinish(statementId)
  }

  private def dropSqlserverTables(): Unit = {
    val tableVar = sqlContext.sparkSession.getSqlServerTable.get(1)
    if (null != tableVar) {
      val iterator = tableVar.keySet().iterator()
      while (iterator.hasNext) {
        sqlContext.sparkSession.sql(" DROP TABLE  IF EXISTS " + tableVar.get(iterator.next()))
      }
    }
  }

  private def clearCrudTableMapForSqlServer(plans: java.util.List[LogicalPlan]) = {
    plans.toArray.foreach(p => {
      clearCrudTableMap(p.asInstanceOf[LogicalPlan])
    })
  }

  private def clearCrudTableMap(plan: LogicalPlan) = {

    if (plan.isInstanceOf[AcidDelCommand]) {
      val tableIdent = plan.asInstanceOf[AcidDelCommand].tableIdentifier
      val fullTableName: String = sqlContext.sparkSession.getFullTableName(tableIdent)
      logInfo(s" del tableName:" + fullTableName)
      sqlContext.sparkContext.crudTbOperationRecordMap.remove(fullTableName)
    } else if (plan.isInstanceOf[AcidUpdateCommand]) {
      val tableIdent = plan.asInstanceOf[AcidUpdateCommand].tableIdent
      val fullTableName: String = sqlContext.sparkSession.getFullTableName(tableIdent)
      logInfo(s" up tableName:" + fullTableName)
      sqlContext.sparkContext.crudTbOperationRecordMap.remove(fullTableName)
    } else if (plan.isInstanceOf[InsertIntoTable]) {
      val tableName = plan.asInstanceOf[InsertIntoTable].tableName
      val dbName = plan.asInstanceOf[InsertIntoTable].dbName
      val identifier = new TableIdentifier(tableName, dbName)
      val fullTableName = sqlContext.sparkSession.getFullTableName(identifier)
      val tableMetadata = sqlContext.sessionState.catalog.getTableMetadata(
        new TableIdentifier(tableName, dbName))
      logInfo(s" insert tableName:" + fullTableName)
      if (sqlContext.sessionState.catalog.checkAcidTable(tableMetadata)) {
        sqlContext.sparkContext.crudTbOperationRecordMap.remove(fullTableName)
      }
    }
  }

  override def cancel(): Unit = {
    logInfo(s"Cancel '$statement' with $statementId")
    if (statementId != null) {
      sqlContext.sparkContext.cancelJobGroup(statementId)
    }
    cleanup(OperationState.CANCELED)
  }

  private def cleanup(state: OperationState) {
    setState(state)
    if (runInBackground) {
      val backgroundHandle = getBackgroundHandle()
      if (backgroundHandle != null) {
        backgroundHandle.cancel(true)
      }
    }
  }
}

object SparkExecuteStatementOperation {
  def getTableSchema(structType: StructType): TableSchema = {
    val schema = structType.map { field =>
      val attrTypeString = if (field.dataType == NullType) "void" else field.dataType.catalogString
      new FieldSchema(field.name, attrTypeString, "")
    }
    new TableSchema(schema.asJava)
  }
}
