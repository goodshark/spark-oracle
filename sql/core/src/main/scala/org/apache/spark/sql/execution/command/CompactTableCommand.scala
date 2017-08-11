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

package org.apache.spark.sql.execution.command

import java.io.{DataInput, DataOutput}
import java.util.UUID
import java.util.regex.Matcher

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.common.ValidReadTxnList
import org.apache.hadoop.hive.common.ValidTxnList
import org.apache.hadoop.hive.common.ValidTxnList.RangeResponse
import org.apache.hadoop.hive.ql.io.{AcidInputFormat, AcidOutputFormat, AcidUtils, RecordIdentifier}
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.hadoop.io.{NullWritable, Writable, WritableComparable}
import org.apache.hadoop.mapred._

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.scheduler._
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, CatalogTable, CatalogTablePartition}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.{LogicalRelation, PartitioningUtils}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration

case class CompactTableCommand(isMajor: Boolean,
                               tableIdent: TableIdentifier,
                               partSpec: Option[TablePartitionSpec],
                               txns: ValidTxnList) extends RunnableCommand {

  private val PREFIX = "hive.compactor."
  private val BASE_DIR = PREFIX + "input.dir"
  private val TMP_OUTPUT_DIR = PREFIX + "tmp.output.dir"
  private val TXNS = PREFIX + "txns"
  private val ISMAJOR = PREFIX + "major"
  private val ISCOMPRESS = PREFIX + "is.compressed"
  private val INPUT_FORMAT = PREFIX + "input.format.class.name"
  private val OUTPUT_FORMAT = PREFIX + "output.format.class.name"
  private val TBL_PROPS = PREFIX + "table.props"
  private val NUMBUCKETS = PREFIX + "num.buckets"
  private val VALIDTXN = "hive.txn.valid.txns"


  override def output: Seq[Attribute] = StructType(
    StructField("compact result", StringType, nullable = false) :: Nil).toAttributes

  private def resolveProperties(sparkSession: SparkSession, table: CatalogTable,
                                partion : Option[CatalogTablePartition] ) = {
    val location = {
      if ( partion.isDefined && partion.get.storage.locationUri.isDefined ) {
        partion.get.storage.locationUri.get.toString
      } else {
        table.storage.locationUri.get.toString
      }
    }

    val hadoopConf = sparkSession.sessionState.newHadoopConf()
    var stagingDir = hadoopConf.get("hive.exec.stagingdir", ".hive-staging")
    val tmplocation =
      if ( stagingDir.startsWith(".hive-staging")) {
        location + "/.hive-staging-" + UUID.randomUUID().toString
      } else {
        stagingDir + "/.hive-staging-" + UUID.randomUUID().toString
      }


    val ( cols, coltypes ) = table.schema.fields.map {
      x => ( x.name, x.dataType.catalogString )
    }.unzip

    Map[String, String] (
      BASE_DIR -> location,
      TMP_OUTPUT_DIR -> tmplocation,
      INPUT_FORMAT -> table.storage.inputFormat.getOrElse("").toString,
      OUTPUT_FORMAT -> table.storage.outputFormat.getOrElse("").toString,
      ISCOMPRESS -> table.storage.compressed.toString,
      ISMAJOR -> isMajor.toString,
      TXNS -> txns.writeToString(),
      VALIDTXN -> txns.writeToString(),
      TBL_PROPS -> CompactTableCommand.mapToString( table.storage.properties ),
      "columns" -> cols.mkString(","),
      "columns.types" -> coltypes.mkString(":")
    )
  }

  private def assertCrudTable( table: CatalogTable ) = {
    val hasBucket = table.bucketSpec.isDefined
    val isOrcformat = table.storage.outputFormat.getOrElse("TextFile").equals(
      "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
    val isTranscational = table.properties.getOrElse("transactional", "false")
      .equalsIgnoreCase("true")
    if ( !hasBucket || ! isOrcformat || ! isTranscational ) {
      throw new AnalysisException(s"compact table ${table.identifier} not a crud table ," +
        s"hasBucket:$hasBucket, isOrcformat:$isOrcformat,isTranscational: $isTranscational" )
    }
    val sortcolumns = table.bucketSpec.get.sortColumnNames
    if ( sortcolumns.nonEmpty ) {
      throw new AnalysisException(s"compact table ${table.identifier} " +
        s"have sort columns $sortcolumns ," +
        "is not yet supported!" )
    }
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val sessionState = sparkSession.sessionState
    val relation = EliminateSubqueryAliases(sessionState.catalog.lookupRelation(tableIdent))
    val table: CatalogTable = relation match {
      case re : CatalogRelation => re.catalogTable
      case lg: LogicalRelation if lg.catalogTable.isDefined => lg.catalogTable.get
      case otherRelation =>
        throw new AnalysisException("compact TABLE is not supported for " +
        s"${otherRelation.nodeName}.")
    }
    assertCrudTable( table )

    val partition = partSpec.map( sessionState.catalog.getPartition(table.identifier, _))
    if ( partSpec.isDefined && partition.isEmpty ) {
      throw new AnalysisException(s"compact table ${table.identifier} " +
        s"can't find partition $partSpec")
    }

    val properties = resolveProperties(sparkSession, table, partition )

    runInteralWithProperites(sparkSession, properties )

  }

  private def runInteralWithProperites( sparkSession: SparkSession,
                                        props: Map[String, String]): Seq[Row] = {
    logInfo("compact with properties:" + props)

    val _conf = sparkSession.sessionState.newHadoopConfWithOptions(props)

    val broadcastConf = sparkSession.sparkContext.broadcast(new SerializableConfiguration(_conf))

    val rdd = new HadoopRDD[NullWritable, CompactorInputSplit] (
      sparkSession.sparkContext,
      broadcastConf.asInstanceOf[Broadcast[SerializableConfiguration]],
      None,
      classOf[CompactorInputFormat],
      classOf[NullWritable],
      classOf[CompactorInputSplit],
      0 )

    val res = rdd.map(_._2).mapPartitions { itr =>
        itr.map { split =>
          logInfo( "Trying to compact split for: " + split.toString )

          val jobConf = broadcastConf.value.value
          val txnList = ValidCompactorTxnList()
          txnList.readFromString( jobConf.get(TXNS) )

          val isMajor = jobConf.getBoolean(ISMAJOR, false)

         val inputClass = jobConf.getClass(INPUT_FORMAT,
              classOf[AcidInputFormat[ WritableComparable[_], Writable]])

          val aif = inputClass.newInstance()
            .asInstanceOf[AcidInputFormat[WritableComparable[_], Writable]]

          val reader = aif.getRawReader(jobConf, isMajor,
                      split.getBucket, txnList, split.getBaseDir, split.getDeltaDirs )

          val identifier: RecordIdentifier = reader.createKey()
          val value: Writable = reader.createValue()

          val outputClass = jobConf.getClass(OUTPUT_FORMAT,
            classOf[AcidOutputFormat[ WritableComparable[_], Writable]])

          val aof = outputClass.newInstance()
            .asInstanceOf[AcidOutputFormat[WritableComparable[_], Writable]]

          val options = new AcidOutputFormat.Options(jobConf)
          options.inspector(reader.getObjectInspector).
            writingBase(isMajor).
            isCompressed(jobConf.getBoolean(ISCOMPRESS, false)).
            tableProperties(CompactTableCommand.stringToProperties(jobConf.get(TBL_PROPS))).
            minimumTransactionId(split.getTxnMin).
            maximumTransactionId(split.getTxnMax).
            bucket(split.getBucket)

          val writer = aof.getRawRecordWriter(new Path(jobConf.get(TMP_OUTPUT_DIR)), options)
          while( reader.next(identifier, value)) {
            if ( isMajor && reader.isDelete(value) ) {
                // don't write out deleted record.
            } else {
              writer.write(value)
             }
          }
          writer.close(false)
          reader.close()
          split.getBucket
      }
    }

    val tmpdir = _conf.get(TMP_OUTPUT_DIR)
    val dest = _conf.get(BASE_DIR)

    val  sRddid = res.id.toString
    sparkSession.sparkContext.setLocalProperty("compact-for-rdd-id", sRddid )

    val listener = new SparkListener {
      var current_compactor_jobid = 0
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        val rddid = jobStart.properties.getProperty("compact-for-rdd-id")
        logInfo(s"onJobStart:jobid = ${jobStart.jobId}," +
          s" rdd in property = ${rddid} , compact rdd id = ${sRddid}" )

        if ( sRddid == rddid ) {
          current_compactor_jobid = jobStart.jobId
        }
      }
      override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
        logInfo(s"onJobEnd: jobid=${jobEnd.jobId}, saved jobid = ${current_compactor_jobid}" )
        if ( current_compactor_jobid == jobEnd.jobId ) {
            logInfo("compactor job ended with status " + jobEnd.jobResult )
            jobEnd.jobResult match {
              case JobSucceeded =>
                logInfo(s"compact success , trying to move $tmpdir to dest $dest")
                val destpath = new Path( dest )
                val tmppath = new Path( tmpdir )
                val fs = tmppath.getFileSystem( _conf )
                fs.listStatus( tmppath ).foreach { fp =>
                  val newpath = new Path( destpath, fp.getPath.getName )
                  fs.rename( fp.getPath, newpath )
                }
                fs.delete( tmppath, true )

              case JobFailed(e) =>
                logInfo(s"compact failed with exception ${e} , trying to remove $tmpdir")
                val tmppath = new Path( tmpdir )
                val fs = tmppath.getFileSystem( _conf )
                fs.delete( tmppath, true )
            }
        }
      }
    }

    logInfo("before add listener , compact rdd id:" + sRddid )
    sparkSession.sparkContext.listenerBus.addListener( listener )

    // do the real job
    val bucket_size = res.collect().length

    sparkSession.sparkContext.listenerBus.removeListener( listener )
    logInfo("after remove listener , compact rdd id:" + res.id.toString )
    sparkSession.sparkContext.setLocalProperty("compact-for-rdd-id", null )

    Seq( Row(bucket_size.toString))

  }

}
object CompactTableCommand extends Logging {

  def stringToProperties(s: String): java.util.Properties = {
    val props = new java.util.Properties
    s.split(",").map(_.split("=")).map {case Array(k, v) => props.put(k, v)}
    props
  }

  def mapToString(m: Map[String, String]): String = {
    m.map( x => s"${x._1}=${x._2}" ).mkString(",")
  }

  def parseToCommand(statement: String) : CompactTableCommand = {
    // compact [major|minor] <db.tbl.part> txns high:min:excepts
    val tokens = statement.trim.split("\\s+")
    if (tokens.length != 5 || tokens(0) != "compact" || tokens(3) != "txns" ) {
      throw new AnalysisException("should have 5 tokens and the 1st and 4th token " +
        "should be compact and txns, but meet " +
        "tokens length" + tokens.length + "," + tokens(0) + "," + tokens(3))
    }
    if ( !(tokens(1) == "major" || tokens(1) == "minor"))  {
      throw new AnalysisException("compact type should be major or minor, but meet : " + tokens(1))
    }

    val ( tableIdent, partSpec ) = parseFullName(tokens(2))
    val txns = parseTxns(tokens(4))
    new CompactTableCommand( tokens(1) == "major", tableIdent, partSpec, txns)
  }

  private def parseFullName(fullname: String) = {
    val tks = fullname.split("\\.")
    if ( tks.length < 2 || tks.length > 3 ) {
      throw new AnalysisException("can't parse table from: " + fullname)
    }
    val tblIdent = new TableIdentifier( tks(1), Option(tks(0)) )
    val parts = if ( tks.length == 3 ) Some(PartitioningUtils.parsePathFragment(tks(2))) else None
    (tblIdent, parts)
  }

  private def parseTxns(txn: String): ValidTxnList = {
    val validTxn = new ValidCompactorTxnList
    validTxn.readFromString(txn)
    validTxn
  }
}

case class ValidCompactorTxnList(var minOpenTxn: Long = -1) extends ValidReadTxnList {

  override def isTxnRangeValid(minTxnId: Long, maxTxnId: Long): RangeResponse = {
    if ( highWatermark < minTxnId ) {
      RangeResponse.NONE
    } else if ( minOpenTxn < 0 ) {
      if ( highWatermark >= maxTxnId ) RangeResponse.ALL else RangeResponse.NONE
    } else {
      if ( minOpenTxn > maxTxnId ) RangeResponse.ALL else RangeResponse.NONE
    }
  }

  override def readFromString(src: String): Unit = {
    if ( src == null ) {
      highWatermark = Long.MaxValue
      exceptions = new Array[Long](0)
    } else {
      val values = src.split(":").map( _.toLong )
      highWatermark = values(0)
      minOpenTxn = values(1)
      exceptions = values.slice(2, values.length)
    }
  }

  override def writeToString(): String = {
    val sb = new StringBuilder
    sb.append(highWatermark).append(":")
    sb.append(minOpenTxn)
    if ( exceptions.length == 0 ) {
      sb.append(":")
    } else {
      exceptions.foreach( sb.append(":").append(_) )
    }
    sb.toString()
  }

  override def toString: String = writeToString()

}

class CompactorInputSplit(var bucket: Int,
                          var txnMin: Long,
                          var txnMax: Long,
                          var length: Long,
                          var base: Path,
                          var deltas: Array[Path],
                          var locations: Array[String]) extends InputSplit {

  def this() = this(-1, -1, -1, -1, null, null, null)

  def init( jobConf: JobConf, files: Array[( Path, Long)] ): CompactorInputSplit = {
    val fs = files(0)._1.getFileSystem( jobConf )
    length = files.map(_._2).sum
    locations = {
      val maxLenFile = files.maxBy( f => f._2 )
      val locs = fs.getFileBlockLocations( maxLenFile._1, 0, maxLenFile._2 )
      locs.flatMap( _.getHosts )
    }
    this
  }
  def set(split: CompactorInputSplit): Unit = {
    bucket = split.bucket
    txnMin = split.txnMin
    txnMax = split.txnMax
    length = split.length
    base = split.base
    deltas = split.deltas
    locations = split.locations
  }

  def getBucket: Int = bucket

  def getBaseDir: Path = base

  def getTxnMin: Long = txnMin

  def getTxnMax: Long = txnMax

  def getDeltaDirs: Array[Path] = deltas

  override def getLength: Long = length

  override def getLocations: Array[String] = locations

  override def toString: String = {
    val sb = new mutable.StringBuilder()
    sb.append("compact split:")
    sb.append("base=").append(base)
    sb.append(",length=").append(length)
    sb.append(",bucket=").append(bucket)
    sb.append(",txnMin=").append(txnMin)
    sb.append(",txnMax=").append(txnMax)
    sb.append(",deltas=").append(deltas.mkString("[", ",", "]"))
    sb.append(",locations=").append(locations.mkString("[", ",", "]"))
    sb.toString()
  }

  override def readFields(dataInput: DataInput): Unit = {
    length = dataInput.readLong()
    val locsize = dataInput.readInt()
    locations = Array.ofDim[String](locsize).map { _ =>
      val buf = Array.ofDim[Byte]( dataInput.readInt())
      dataInput.readFully(buf)
      new String(buf)
    }
    bucket = dataInput.readInt()
    txnMin = dataInput.readLong()
    txnMax = dataInput.readLong()
    val baselen = dataInput.readInt()
    base = if ( baselen > 0 ) {
      val buf = Array.ofDim[Byte]( baselen )
      dataInput.readFully(buf)
      new Path(new String(buf))
    } else {
      null
    }
    val deltasize = dataInput.readInt()
    deltas = Array.ofDim[Path](deltasize).map { _ =>
      val buf = Array.ofDim[Byte]( dataInput.readInt())
      dataInput.readFully(buf)
      new Path( new String(buf) )
    }
  }

  override def write(dataOutput: DataOutput): Unit = {
    dataOutput.writeLong( length )
    dataOutput.writeInt( locations.size )
    locations.foreach{ loc =>
      dataOutput.writeInt(loc.length)
      dataOutput.writeBytes(loc)
    }
    dataOutput.writeInt( bucket )
    dataOutput.writeLong( txnMin )
    dataOutput.writeLong( txnMax )
    if ( base == null ) {
      dataOutput.writeInt(0)
    } else {
      dataOutput.writeInt(base.toString.length)
      dataOutput.writeBytes(base.toString)
    }
    dataOutput.writeInt( deltas.size )
    deltas.foreach{ dt =>
      dataOutput.writeInt(dt.toString.length)
      dataOutput.writeBytes(dt.toString)
    }
  }
}

class CompactorRecordReader(var split: CompactorInputSplit)
          extends RecordReader[NullWritable, CompactorInputSplit] {

  override def next(k: NullWritable, v: CompactorInputSplit): Boolean = {
    if ( split != null ) {
      v.set( split)
       split = null
      true
    } else {
      false
    }
  }

  override def createKey(): NullWritable = NullWritable.get()

  override def getProgress: Float = 0

  override def getPos: Long = 0

  override def createValue(): CompactorInputSplit = new CompactorInputSplit()

  override def close(): Unit = {
  }
}

class CompactorInputFormat extends InputFormat[NullWritable, CompactorInputSplit] with  Logging {
  override def getRecordReader(inputSplit: InputSplit, jobConf: JobConf, reporter: Reporter):
    RecordReader[NullWritable, CompactorInputSplit] = {
    new CompactorRecordReader( inputSplit.asInstanceOf[CompactorInputSplit] )
  }

  override def getSplits(jobConf: JobConf, i: Int): Array[InputSplit] = {
    val txns = ValidCompactorTxnList()
    txns.readFromString(jobConf.get("hive.compactor.txns"))
    val inputdir = new Path(jobConf.get("hive.compactor.input.dir"))
    val isMajor = jobConf.getBoolean("hive.compactor.major", false)

    val acidDir = AcidUtils.getAcidState(inputdir, jobConf, txns)
    val dirsToSearch = new mutable.ArrayBuffer[Path]()
    val dirsDelta = new mutable.ArrayBuffer[Path]()

    val basedir = isMajor match {
      case false => null
      case true =>
        acidDir.getBaseDirectory match {
          case someDir: Path =>
            dirsToSearch += someDir
            someDir
          case _ =>
            acidDir.getOriginalFiles.asScala.foreach { e =>
              dirsToSearch += e.getPath
            }
            inputdir
        }
    }

    val deltas = acidDir.getCurrentDirectories.asScala
    if ( deltas.isEmpty ) {
      logInfo("No delta files found to compact in " + inputdir )
      return Array.empty[InputSplit]
    }

    // get txn ranges
    var minTxn = Long.MaxValue
    var maxTxn = Long.MinValue
    deltas.foreach { delta =>
      dirsToSearch += delta.getPath
      dirsDelta += delta.getPath
      minTxn = Math.min(minTxn, delta.getMinTransaction)
      maxTxn = Math.max(maxTxn, delta.getMaxTransaction)
    }
       // list files
    val buckets = new BucketTracker(basedir, dirsToSearch, jobConf).retrive
    val splits = buckets.map { item =>  // ( bucket number, basedir , files)
      new CompactorInputSplit(item._1, minTxn, maxTxn, -1, item._2,
                dirsDelta.toArray, Array.empty[String] ).init( jobConf, item._3 )
    }.toArray[InputSplit]
    logInfo(s"total splits: ${splits.length}")
    splits
  }

  class BucketTracker(basedir: Path, dirsToRetrive: mutable.ArrayBuffer[Path], jobConf: JobConf)
    extends Logging {

    private lazy val fs = dirsToRetrive(0).getFileSystem(jobConf)
    // bucketnumber -> a list files belong to this bucket )
    private val bucketFileMaps = mutable.HashMap[Int, mutable.ArrayBuffer[( Path, Long) ]]()
    private val bucketSawBase = mutable.HashMap[Int, Boolean]()

    private def addFile( matcher: Matcher, file: Path, filelen: Long, sawbase: Boolean) = {
      if ( !matcher.find()) {
        logWarning(s"Found a non-bucket file : $file")
      }
      val bucketNum = matcher.group().toInt
      if ( !bucketFileMaps.contains(bucketNum) ) {
        bucketFileMaps.put(bucketNum, mutable.ArrayBuffer( ( file, filelen) ) )
        bucketSawBase.put(bucketNum, sawbase)
      } else {
        val buffer: mutable.ArrayBuffer[( Path, Long) ] = bucketFileMaps.get(bucketNum).get
        buffer.+= (( file, filelen ))

        val newval = bucketSawBase.getOrElse(bucketNum, false) | sawbase
        bucketSawBase.put(bucketNum, newval)
      }
    }

    def retrive: Iterator[(Int, Path, Array[( Path, Long)])] = {
      dirsToRetrive.foreach { dir =>
        if ( dir.getName.startsWith( AcidUtils.BASE_PREFIX ) ||
          dir.getName.startsWith( AcidUtils.DELTA_PREFIX)) {
          val sawbase = dir.getName.startsWith( AcidUtils.BASE_PREFIX )
          val files = fs.listStatus(dir, AcidUtils.bucketFileFilter )
          files.foreach { f =>
            val matcher = AcidUtils.BUCKET_DIGIT_PATTERN.matcher( f.getPath.getName )
            addFile( matcher, f.getPath, f.getLen, sawbase )
          }
        } else {
          val ss = fs.getFileStatus(dir)
          val matcher = AcidUtils.LEGACY_BUCKET_DIGIT_PATTERN.matcher(dir.getName)
          addFile( matcher, dir, ss.getLen, true )
        }
      }
      bucketFileMaps.map{ kv =>
        val _basedir = if ( bucketSawBase.getOrElse(kv._1, false) ) basedir else null
        ( kv._1, _basedir, kv._2.toArray )
      }.toIterator
    }
  }
}