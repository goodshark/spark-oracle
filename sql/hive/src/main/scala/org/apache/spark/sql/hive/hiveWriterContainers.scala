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

package org.apache.spark.sql.hive


import java.text.NumberFormat
import java.util
import java.util.{Date, Locale, Properties}

import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.exec.{FileSinkOperator, Utilities}
import org.apache.hadoop.hive.ql.io._
import org.apache.hadoop.hive.ql.plan.TableDesc
import org.apache.hadoop.hive.serde2.{Deserializer, Serializer}
import org.apache.hadoop.hive.serde2.objectinspector._
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption
import org.apache.hadoop.hive.serde2.typeinfo.{TypeInfo, TypeInfoFactory, TypeInfoUtils}
import org.apache.hadoop.io.{LongWritable, Writable}
import org.apache.hadoop.mapred._
import org.apache.hadoop.mapreduce.TaskType
import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.mapred.SparkHadoopMapRedUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.execution.UnsafeKVExternalSorter
import org.apache.spark.sql.execution.datasources.jdbc.JdbcUtils
import org.apache.spark.sql.hive.HiveShim.{ShimFileSinkDesc => FileSinkDesc}
import org.apache.spark.sql.types.{DataType, _}
import org.apache.spark.util.SerializableJobConf
import org.apache.spark.util.collection.unsafe.sort.UnsafeExternalSorter

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Internal helper class that saves an RDD using a Hive OutputFormat.
 * It is based on `SparkHadoopWriter`.
 */
private[hive] class SparkHiveWriterContainer(
                                              @transient private val jobConf: JobConf,
                                              fileSinkConf: FileSinkDesc,
                                              inputSchema: Seq[Attribute],
                                              table: MetastoreRelation)
  extends Logging
  with HiveInspectors
  with Serializable {

  private val now = new Date()
  private val tableDesc: TableDesc = fileSinkConf.getTableInfo
  // Add table properties from storage handler to jobConf, so any custom storage
  // handler settings can be set to jobConf
  if (tableDesc != null) {
    HiveTableUtil.configureJobPropertiesForStorageHandler(tableDesc, jobConf, false)
    Utilities.copyTableJobPropertiesToConf(tableDesc, jobConf)
  }
  protected val conf = new SerializableJobConf(jobConf)

  private var jobID = 0
  private var splitID = 0
  private var attemptID = 0
  private var jID: SerializableWritable[JobID] = null
  private var taID: SerializableWritable[TaskAttemptID] = null

  @transient private var writer: FileSinkOperator.RecordWriter = null
  @transient protected lazy val committer = conf.value.getOutputCommitter
  @transient protected lazy val jobContext = new JobContextImpl(conf.value, jID.value)
  @transient private lazy val taskContext = new TaskAttemptContextImpl(conf.value, taID.value)
  @transient private lazy val outputFormat =
    conf.value.getOutputFormat.asInstanceOf[HiveOutputFormat[AnyRef, Writable]]

  def driverSideSetup() {
    setIDs(0, 0, 0)
    setConfParams()
    committer.setupJob(jobContext)
  }

  def executorSideSetup(jobId: Int, splitId: Int, attemptId: Int) {
    setIDs(jobId, splitId, attemptId)
    setConfParams()
    committer.setupTask(taskContext)
    initWriters()
  }

  protected def getOutputName: String = {
    val numberFormat = NumberFormat.getInstance(Locale.US)
    numberFormat.setMinimumIntegerDigits(5)
    numberFormat.setGroupingUsed(false)
    val extension = Utilities.getFileExtension(conf.value, fileSinkConf.getCompressed, outputFormat)
    "part-" + numberFormat.format(splitID) + extension
  }

  def close() {
    // Seems the boolean value passed into close does not matter.
    if (writer != null) {
      writer.close(false)
      commit()
    }
  }

  def commitJob() {
    committer.commitJob(jobContext)
  }

  protected def initWriters() {
    // NOTE this method is executed at the executor side.
    // For Hive tables without partitions or with only static partitions, only 1 writer is needed.
    writer = HiveFileFormatUtils.getHiveRecordWriter(
      conf.value,
      fileSinkConf.getTableInfo,
      conf.value.getOutputValueClass.asInstanceOf[Class[Writable]],
      fileSinkConf,
      FileOutputFormat.getTaskOutputPath(conf.value, getOutputName),
      Reporter.NULL)
  }

  protected def commit() {
    SparkHadoopMapRedUtil.commitTask(committer, taskContext, jobID, splitID)
  }

  def abortTask(): Unit = {
    if (committer != null) {
      committer.abortTask(taskContext)
    }
    logError(s"Task attempt $taskContext aborted.")
  }

  private def setIDs(jobId: Int, splitId: Int, attemptId: Int) {
    jobID = jobId
    splitID = splitId
    attemptID = attemptId

    jID = new SerializableWritable[JobID](SparkHadoopWriter.createJobID(now, jobId))
    taID = new SerializableWritable[TaskAttemptID](
      new TaskAttemptID(new TaskID(jID.value, TaskType.MAP, splitID), attemptID))
  }

  private def setConfParams() {
    conf.value.set("mapred.job.id", jID.value.toString)
    conf.value.set("mapred.tip.id", taID.value.getTaskID.toString)
    conf.value.set("mapred.task.id", taID.value.toString)
    conf.value.setBoolean("mapred.task.is.map", true)
    conf.value.setInt("mapred.task.partition", splitID)
  }

  def newSerializer(tableDesc: TableDesc): Serializer = {
    val serializer = tableDesc.getDeserializerClass.newInstance().asInstanceOf[Serializer]
    serializer.initialize(null, tableDesc.getProperties)
    serializer
  }

  protected def prepareForWrite() = {
    val serializer = newSerializer(fileSinkConf.getTableInfo)
    val standardOI = ObjectInspectorUtils
      .getStandardObjectInspector(
        fileSinkConf.getTableInfo.getDeserializer.getObjectInspector,
        ObjectInspectorCopyOption.JAVA)
      .asInstanceOf[StructObjectInspector]

    val fieldOIs = standardOI.getAllStructFieldRefs.asScala.map(_.getFieldObjectInspector).toArray
    val fieldsLen = fieldOIs.length
    val allDataTypes = table.catalogTable.schema.map(_.dataType)
    val dataTypes: ListBuffer[DataType] = new ListBuffer[DataType]
    for(index <- 0 to (fieldsLen - 1)) {
      dataTypes += allDataTypes(index)
    }
//    val dataTypes = buf.toSeq
//    val dataTypes = allDataTypes.filter({ index+=1; index < fieldsLen })
//    val dataTypes = inputSchema.map(_.dataType)
//    val dataTypes = fieldOIs.map(in => DataType.nameToType(in.getTypeName))
    val wrappers = fieldOIs.zip(dataTypes).map { case (f, dt) => wrapperFor(f, dt) }
    val outputData = new Array[Any](fieldOIs.length)
    (serializer, standardOI, fieldOIs, dataTypes, wrappers, outputData)
  }

  def getUpdateInspector: StandardStructObjectInspector = {
    val ois = new util.ArrayList[ObjectInspector]()
    val vidStructName = new util.ArrayList[String]()
    vidStructName.add("transactionId")
    vidStructName.add("bucketId")
    vidStructName.add("rowId")

    val vidStructType = new util.ArrayList[TypeInfo]()
    vidStructType.add(TypeInfoFactory.getPrimitiveTypeInfo("bigint"))
    vidStructType.add(TypeInfoFactory.getPrimitiveTypeInfo("int"))
    vidStructType.add(TypeInfoFactory.getPrimitiveTypeInfo("bigint"))
    ois.add(0, TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
      TypeInfoFactory.getStructTypeInfo(
        vidStructName,
        vidStructType)))

    val colNames = new util.ArrayList[String]()
    colNames.add("_col0")
    var index = 1
    table.catalogTable.schema.foreach {
      f =>
        colNames.add("_col" + index)
       /* ois.add(index, TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
          TypeInfoFactory.getPrimitiveTypeInfo(f.dataType.typeName))
        ) */

        ois.add(index, TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
          TypeInfoFactory.getPrimitiveTypeInfo(getDataTypeNameToHive(f.dataType.typeName)))
        )
        index = index + 1
    }
    ObjectInspectorFactory.getStandardStructObjectInspector(colNames, ois)
  }

  def getInsertInspector: StandardStructObjectInspector = {
    val ois = new util.ArrayList[ObjectInspector]()
    val colNames = new util.ArrayList[String]()
    var index = 0
    table.catalogTable.schema.foreach {
      f =>
        colNames.add("_col" + index)
       /* ois.add(index, TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
          TypeInfoFactory.getPrimitiveTypeInfo(f.dataType.typeName))
        )
        */

        ois.add(index, TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(
          TypeInfoFactory.getPrimitiveTypeInfo(getDataTypeNameToHive(f.dataType.typeName)))
        )
        index = index + 1
    }
    ObjectInspectorFactory.getStandardStructObjectInspector(colNames, ois)
  }


  def getDataTypeNameToHive( dataType: String) : String = {
    var rs = dataType
    if (dataType.equalsIgnoreCase("long")) {
      rs = "bigint"
    } else if (dataType.equalsIgnoreCase("integer")) {
      rs = "int"
    } else if (dataType.equalsIgnoreCase("byte")) {
      rs = "binary"
    } else if (dataType.equalsIgnoreCase("short")) {
      rs = "tinyint"
    }
    logDebug("dataType src is " + dataType + "is + " + rs)
    rs
  }

  def getTransactionId: Long = {
    val url = conf.value.get("javax.jdo.option.ConnectionURL")
    val props = new Properties()
    props.put("user", conf.value.get("javax.jdo.option.ConnectionUserName"))
    props.put("password", conf.value.get("javax.jdo.option.ConnectionPassword"))
    val conn = JdbcUtils.createConnectionFactory(url, props)()
    try {
      var sql = "lock tables NEXT_TXN_ID WRITE, TXNS WRITE "
      val lockStmt = conn.prepareStatement(sql)
      lockStmt.executeQuery(sql)
      sql = "select ntxn_next from NEXT_TXN_ID"
      val stmt = conn.prepareStatement(sql)
      val rs = stmt.executeQuery(sql)
      if (!rs.next()) {
        throw new Exception("Transaction database not properly " +
          "configured, can't find next transaction id.")
      }
      val first: Long = rs.getLong(1)
      logDebug(s" get current txnId : ${first}")
      val txnId = first + 1
      sql = "update NEXT_TXN_ID set ntxn_next = " + txnId
      logDebug(s" update table NEXT_TXN_ID sql [ ${sql} ]")
      stmt.executeUpdate(sql)
      /* val now = System.currentTimeMillis()
      val userName = "spark_txnId_user"
      val hostName = "spark_txId_hostName"
      sql = "insert into TXNS (txn_id, txn_state, txn_started, " +
        "txn_last_heartbeat, txn_user, txn_host) values (?, 'o', " + now + ", " +
        now + ", '" + userName + "', '" + hostName + "')"
      logInfo(s" insert table TXNS sql [ ${sql} ]")
      val ps = conn.prepareStatement(sql)
      ps.setLong(1, txnId)
      ps.executeUpdate() */
      sql = "unlock tables"
      val unLockStmt = conn.prepareStatement(sql)
      unLockStmt.executeQuery(sql)
      first
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }

  // completed file
  def completedFile(txnId: Long, database: String, tableName: String, partition: String): Unit = {
    val url = conf.value.get("javax.jdo.option.ConnectionURL")
    val props = new Properties()
    props.put("user", conf.value.get("javax.jdo.option.ConnectionUserName"))
    props.put("password", conf.value.get("javax.jdo.option.ConnectionPassword"))
    var p = ""
    if (partition.isEmpty) {
      p = "NULL"
    } else {
      p = "'" + partition + "'"
    }
    val conn = JdbcUtils.createConnectionFactory(url, props)()
    try {
      val sql = "insert into COMPLETED_TXN_COMPONENTS ( ctc_txnid, ctc_database, ctc_table, " +
        "ctc_partition ) VALUES ( " +
        txnId + " ,'" + database + "','" + tableName + "'," + p + " )"
      logDebug(s" insert into  table COMPLETED_TXN_COMPONENTS sql [ ${sql} ]")
      val stmt = conn.prepareStatement(sql)
      stmt.executeUpdate(sql)
    } finally {
      if (conn != null) {
        conn.close()
      }
    }
  }


  val SPARK_TRANSACTION_ACID: String = "spark.transaction.acid"
  val OPTION_TYPE: String = "OPTION_TYPE"

  private def eliminateVidColumn(originPositions: Array[Int], vidPosition: Int) : Array[Int] = {
    val ret: ListBuffer[Int] = new ListBuffer[Int]
    if (!originPositions.isEmpty) {
      for (i <- 0 to originPositions.length) {
        if (vidPosition != i) {
          ret += originPositions(i)
        }

      }
    }
    ret.toArray
  }

  // this function is executed on executor side
  def writeToFile(context: TaskContext, iterator: Iterator[InternalRow]): Unit = {
    // for insert with column test
    val insertPositions = conf.value.getInts("spark.exe.insert.positions")
//    logError(s"read from conf: insert positions " + insertPositions.mkString(","));
    // end for insert column test

    val transaction_acid_flg = conf.value.get(SPARK_TRANSACTION_ACID, "false")
    if (transaction_acid_flg.equalsIgnoreCase("true")) {

      val (serializer, standardOI, fieldOIs, dataTypes, wrappers, outputData) = prepareForWrite()
      val positions = insertPositions.slice(-1, insertPositions.length -1 )
      logInfo("Final positions: " + positions.mkString(","))
      conf.value.set("spark.exe.insert.positions", positions.mkString(","))
      var partitionPath = ""
      if (null != table.tableDesc.getProperties.getProperty("partition_columns") &&
        table.tableDesc.getProperties.getProperty("partition_columns").nonEmpty) {
        partitionPath = conf.value.get("spark.partition.value")
      }
//      val outPutPath = new Path(
//           FileOutputFormat.getOutputPath(conf.value).toString + partitionPath
      val outPutPath = new Path(
        FileOutputFormat.getOutputPath(conf.value).toString
      )

      val bucketColumnNames = table.catalogTable.
        bucketSpec.getOrElse(new BucketSpec(-1, Seq(), Seq())).bucketColumnNames
      val bucketNumBuckets = table.catalogTable.
        bucketSpec.getOrElse(new BucketSpec(-1, Seq(), Seq())).numBuckets
      var transactionId: Long = -1
      val rows = new util.ArrayList[Any]
      val updateRecordMap = scala.collection.mutable.Map[Integer, RecordUpdater]()
      if (iterator.nonEmpty) {
        transactionId = getTransactionId
        fileSinkConf.setTransactionId(transactionId)
      }
      iterator.foreach {
        row => {
          rows.clear()
          val bucketId: Int = rowsAddVid(bucketColumnNames, bucketNumBuckets,
            standardOI, fieldOIs, rows, row)
          var i = 0
          while (i < fieldOIs.length) {
//            outputData(i) = if (row.isNullAt(i)) null else wrappers(i)(row.get(i, dataTypes(i)))
            outputData(i) = {
              buildOutputData(positions, dataTypes, wrappers, row, i)
            }
            rows.add(outputData(i))
            i += 1
          }

          conf.value.get(OPTION_TYPE) match {
            case "1" =>
              getUpdateRecord(bucketId, getUpdateInspector, outPutPath,
                updateRecordMap, conf.value.get(OPTION_TYPE))
              updateRecordMap.get(bucketId).get.update(transactionId, rows)
            case "2" =>
              getUpdateRecord(bucketId, getUpdateInspector, outPutPath,
                updateRecordMap, conf.value.get(OPTION_TYPE))
              updateRecordMap.get(bucketId).get.delete(transactionId, rows)
            case "0" =>
              getUpdateRecord(bucketId, getInsertInspector, outPutPath,
                updateRecordMap, conf.value.get(OPTION_TYPE))
              updateRecordMap.get(bucketId).get.insert(transactionId, rows)
            case _ =>
              logError(s" no operation ")
          }
        }
      }
      closeUpdateRecord(updateRecordMap)
      completedFile(transactionId, table.databaseName, table.tableName, partitionPath)

    } else {
      val (serializer, standardOI, fieldOIs, dataTypes, wrappers, outputData) = prepareForWrite()
      executorSideSetup(context.stageId, context.partitionId, context.attemptNumber)
      iterator.foreach { row =>
        var i = 0
        while (i < fieldOIs.length) {
          outputData(i) = {
            buildOutputData(insertPositions, dataTypes, wrappers, row, i)
          }
          i += 1
        }
        writer.write(serializer.serialize(outputData, standardOI))
      }
      close()
    }
  }

  private def buildOutputData(insertPositions: Array[Int],
                              dataTypes: ListBuffer[DataType],
                              wrappers: Array[(Any) => Any], row: InternalRow, i: Int) = {
    if (row.isNullAt(i)) {
      null
    } else {
      if (insertPositions.isEmpty) {
        wrappers(i)(row.get(i, dataTypes(i)))
      } else {
        val columnIndex = getInsertIndex(i, insertPositions)
        if (-1 == columnIndex) {
          null
        } else {
          wrappers(i)(row.get(columnIndex, dataTypes(i)))
        }
      }
    }
  }

  def getInsertIndex(index: Int, positions: Array[Int]): Int = {
    return positions.indexOf(index)

  }

  def closeUpdateRecord(updateRecordMap: mutable.Map[Integer, RecordUpdater]): Unit = {
    updateRecordMap.keys.foreach(u => {
      val updateRecord = updateRecordMap.get(u).get
      if (updateRecord != null) {
        updateRecord.close(false)
      }
    })
  }


  def rowsAddVid(bucketColumnNames: Seq[String],
                 bucketNumber: Int,
                 standardOI: StructObjectInspector,
                 fieldOIs: Array[ObjectInspector],
                 rows: util.ArrayList[Any],
                 row: InternalRow): Int = {

    var bucketId: Int = 0
    if (conf.value.get(OPTION_TYPE).equalsIgnoreCase("1")
      || conf.value.get(OPTION_TYPE).equalsIgnoreCase("2")) {
      // val vidValue = row.getString(fieldOIs.length).toString
      val vidValue = row.getString(inputSchema.length-1).toString
      val vidInfo = new util.ArrayList[Object]
      val txnBucketRowId: Array[String] = vidValue.split('^')
      val txnId = new LongWritable(txnBucketRowId(0).trim.toLong)
      val bucketIdWritable = new LongWritable(txnBucketRowId(1).trim.toLong)
      val rowId = new LongWritable(txnBucketRowId(2).trim.toLong)

      logDebug(s" vidValue is : $vidValue txnId: $txnId bucketId: $bucketIdWritable ," +
        s"rowId: $rowId age: ${row.getString(1)}")
      vidInfo.add(txnId)
      vidInfo.add(bucketIdWritable)
      vidInfo.add(rowId)
      rows.add(vidInfo)
      bucketId = txnBucketRowId(1).trim.toInt
    } else {
      val (serializer, standardOI, fieldOIs, dataTypes, wrappers, outputData) = prepareForWrite()
      var hashCode = 0
      try {
        standardOI.getAllStructFieldRefs.asScala.foreach(s => {
          if (bucketColumnNames.contains(s.getFieldName)) {
            val bucketVal = if (row.isNullAt(s.getFieldID)) null
            else wrappers(s.getFieldID)(row.get(s.getFieldID, dataTypes(s.getFieldID)))
            hashCode += bucketVal.hashCode()
          }
        })
      } catch {
        case e: Exception => logError(s" Computer bucket Id error..")
      }
      bucketId = (hashCode & Integer.MAX_VALUE) % bucketNumber
    }
    bucketId
  }


  def getUpdateRecord(bucketId: Int, inspector: StandardStructObjectInspector,
                      outPutPath: Path,
                      updateRecordMap: mutable.Map[Integer, RecordUpdater],
                      op: String): Unit = {
    val rowId: Int = op match {
      case "0" => -1
      case "1" => 0
      case "2" => 0
      case _ => -1
    }
    if (!updateRecordMap.contains(bucketId)) {
      val updateRecord: RecordUpdater = HiveFileFormatUtils.getAcidRecordUpdater(
        conf.value,
        fileSinkConf.getTableInfo,
        // table.catalogTable.numBuckets,
        bucketId,
        fileSinkConf,
        // FileOutputFormat.getTaskOutputPath(conf.value, getOutputName),
        outPutPath,
        inspector,
        Reporter.NULL,
        rowId)
      updateRecordMap.put(bucketId, updateRecord)
    }
  }
}


private[hive] object SparkHiveWriterContainer {
  def createPathFromString(path: String, conf: JobConf): Path = {
    if (path == null) {
      throw new IllegalArgumentException("Output path is null")
    }
    val outputPath = new Path(path)
    val fs = outputPath.getFileSystem(conf)
    if (outputPath == null || fs == null) {
      throw new IllegalArgumentException("Incorrectly formatted output path")
    }
    outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }
}

private[spark] object SparkHiveDynamicPartitionWriterContainer {
  val SUCCESSFUL_JOB_OUTPUT_DIR_MARKER = "mapreduce.fileoutputcommitter.marksuccessfuljobs"
}

private[spark] class SparkHiveDynamicPartitionWriterContainer(
                                                               jobConf: JobConf,
                                                               fileSinkConf: FileSinkDesc,
                                                               dynamicPartColNames: Array[String],
                                                               inputSchema: Seq[Attribute],
                                                               table: MetastoreRelation)
  extends SparkHiveWriterContainer(jobConf, fileSinkConf, inputSchema, table) {

  import SparkHiveDynamicPartitionWriterContainer._

  private val defaultPartName = jobConf.get(
    ConfVars.DEFAULTPARTITIONNAME.varname, ConfVars.DEFAULTPARTITIONNAME.defaultStrVal)

  override protected def initWriters(): Unit = {
    // do nothing
  }

  override def close(): Unit = {
    // do nothing
  }

  override def commitJob(): Unit = {
    // This is a hack to avoid writing _SUCCESS mark file. In lower versions of Hadoop (e.g. 1.0.4),
    // semantics of FileSystem.globStatus() is different from higher versions (e.g. 2.4.1) and will
    // include _SUCCESS file when glob'ing for dynamic partition data files.
    //
    // Better solution is to add a step similar to what Hive FileSinkOperator.jobCloseOp does:
    // calling something like Utilities.mvFileToFinalPath to cleanup the output directory and then
    // load it with loadDynamicPartitions/loadPartition/loadTable.
    val oldMarker = conf.value.getBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, true)
    conf.value.setBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, false)
    super.commitJob()
    conf.value.setBoolean(SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, oldMarker)
  }

  // this function is executed on executor side
  override def writeToFile(context: TaskContext, iterator: Iterator[InternalRow]): Unit = {
    val (serializer, standardOI, fieldOIs, dataTypes, wrappers, outputData) = prepareForWrite()
    executorSideSetup(context.stageId, context.partitionId, context.attemptNumber)

    val partitionOutput = inputSchema.takeRight(dynamicPartColNames.length)
    val dataOutput = inputSchema.take(fieldOIs.length)
    // Returns the partition key given an input row
    val getPartitionKey = UnsafeProjection.create(partitionOutput, inputSchema)
    // Returns the data columns to be written given an input row
    val getOutputRow = UnsafeProjection.create(dataOutput, inputSchema)

    val fun: AnyRef = (pathString: String) => FileUtils.escapePathName(pathString, defaultPartName)
    // Expressions that given a partition key build a string like: col1=val/col2=val/...
    val partitionStringExpression = partitionOutput.zipWithIndex.flatMap { case (c, i) =>
      val escaped =
        ScalaUDF(fun, StringType, Seq(Cast(c, StringType)), Seq(StringType))
      val str = If(IsNull(c), Literal(defaultPartName), escaped)
      val partitionName = Literal(dynamicPartColNames(i) + "=") :: str :: Nil
      if (i == 0) partitionName else Literal(Path.SEPARATOR_CHAR.toString) :: partitionName
    }

    // Returns the partition path given a partition key.
    val getPartitionString =
      UnsafeProjection.create(Concat(partitionStringExpression) :: Nil, partitionOutput)

    // If anything below fails, we should abort the task.
    try {
      val sorter: UnsafeKVExternalSorter = new UnsafeKVExternalSorter(
        StructType.fromAttributes(partitionOutput),
        StructType.fromAttributes(dataOutput),
        SparkEnv.get.blockManager,
        SparkEnv.get.serializerManager,
        TaskContext.get().taskMemoryManager().pageSizeBytes,
        SparkEnv.get.conf.getLong("spark.shuffle.spill.numElementsForceSpillThreshold",
          UnsafeExternalSorter.DEFAULT_NUM_ELEMENTS_FOR_SPILL_THRESHOLD))

      while (iterator.hasNext) {
        val inputRow = iterator.next()
        val currentKey = getPartitionKey(inputRow)
        sorter.insertKV(currentKey, getOutputRow(inputRow))
      }

      logInfo(s"Sorting complete. Writing out partition files one at a time.")
      val sortedIterator = sorter.sortedIterator()
      var currentKey: InternalRow = null
      var currentWriter: FileSinkOperator.RecordWriter = null
      try {
        while (sortedIterator.next()) {
          if (currentKey != sortedIterator.getKey) {
            if (currentWriter != null) {
              currentWriter.close(false)
            }
            currentKey = sortedIterator.getKey.copy()
            logDebug(s"Writing partition: $currentKey")
            currentWriter = newOutputWriter(currentKey)
          }

          var i = 0
          while (i < fieldOIs.length) {
            outputData(i) = if (sortedIterator.getValue.isNullAt(i)) {
              null
            } else {
              wrappers(i)(sortedIterator.getValue.get(i, dataTypes(i)))
            }
            i += 1
          }
          currentWriter.write(serializer.serialize(outputData, standardOI))
        }
      } finally {
        if (currentWriter != null) {
          currentWriter.close(false)
        }
      }
      commit()
    } catch {
      case cause: Throwable =>
        logError("Aborting task.", cause)
        abortTask()
        throw new SparkException("Task failed while writing rows.", cause)
    }
    /** Open and returns a new OutputWriter given a partition key. */
    def newOutputWriter(key: InternalRow): FileSinkOperator.RecordWriter = {
      val partitionPath = getPartitionString(key).getString(0)
      val newFileSinkDesc = new FileSinkDesc(
        fileSinkConf.getDirName + partitionPath,
        fileSinkConf.getTableInfo,
        fileSinkConf.getCompressed)
      newFileSinkDesc.setCompressCodec(fileSinkConf.getCompressCodec)
      newFileSinkDesc.setCompressType(fileSinkConf.getCompressType)

      // use the path like ${hive_tmp}/_temporary/${attemptId}/
      // to avoid write to the same file when `spark.speculation=true`
      val path = FileOutputFormat.getTaskOutputPath(
        conf.value,
        partitionPath.stripPrefix("/") + "/" + getOutputName)

      HiveFileFormatUtils.getHiveRecordWriter(
        conf.value,
        fileSinkConf.getTableInfo,
        conf.value.getOutputValueClass.asInstanceOf[Class[Writable]],
        newFileSinkDesc,
        path,
        Reporter.NULL)
    }
  }
}
