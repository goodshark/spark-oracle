package org.apache.spark

import java.lang.Object
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.SQLContext

import scala.collection.mutable._
import java.util.{Set => javaSet}

import org.apache.hive.tsql.arg.Var

import scala.collection.JavaConversions._

/**
  * Created by dengrb1 on 3/2 0002.
  */

class SqlSessionListener(sqlContext: SQLContext) extends SparkListener with Logging {

  private val sessionToTable = new HashMap[String, HashSet[String]]()

  def onSessionCreated(sessionId: String): Unit = synchronized {
    try {
      logDebug(s"session listener $sessionId created")
      sessionToTable(sessionId) = new HashSet[String]()
      var tmpPackMap = new ConcurrentHashMap[String, ConcurrentHashMap[String, Object]]()
      sqlContext.sparkContext.oraclePackageVars.put(sqlContext.sparkSession, tmpPackMap)
    } catch {
      case e: Exception =>
        logError(s"session $sessionId opening get exception: $e")
    }
  }

  def onSessionClosed(sessionId: String): Unit = synchronized {
    try {
      logDebug(s"session listener $sessionId closed")
      for (table <- sessionToTable(sessionId)) {
        if (sqlContext != null) {
          sqlContext.sql(s"drop table if exists $table")
          logDebug(s"session listener drop table $table")
        } else
          logDebug(s"session listener sqlContext is null, do nothing with table $table")
      }
      sessionToTable.remove(sessionId)
      sqlContext.sparkContext.oraclePackageVars.remove(sqlContext.sparkSession)
    } catch {
      case e: Exception =>
        logError(s"session $sessionId closeing get exception: $e")
    }
  }

  def addTable(sessionId: String, tableSet: javaSet[String]): Unit = {
    try {
      logDebug(s"session listener $sessionId add new tables: $tableSet")
      for (table <- tableSet) {
        sessionToTable(sessionId) += table
      }
    } catch {
      case e: Exception =>
        logError(s"session $sessionId add table get exception: $e")
    }
  }

  def delTable(sessionId: String, tableSet: javaSet[String]): Unit = {
    try {
      logDebug(s"session listener $sessionId del tables: $tableSet")
      for (table <- tableSet) {
        sessionToTable(sessionId) -= table
      }
    } catch {
      case e: Exception =>
        logError(s"session $sessionId del table get exception: $e")
    }
  }

  def mergePackageVars(packVars: ConcurrentHashMap[String, ConcurrentHashMap[String, Var]]): Unit = {
    for (packName <- packVars.keys()) {
      val tmpPackMap = sqlContext.sparkContext.oraclePackageVars.get(sqlContext.sparkSession)
      if (tmpPackMap.containsKey(packName)) {
        val localVarMap = packVars.get(packName)
        val varMap = new ConcurrentHashMap[String, Object]()
        for (vName <- localVarMap.keys()) {
          varMap.put(vName, localVarMap.get(vName))
        }
        tmpPackMap.put(packName, varMap)
      }
    }
  }

}
