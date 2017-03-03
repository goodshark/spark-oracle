package org.apache.spark

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.SparkListener
import org.apache.spark.sql.SQLContext

import scala.collection.mutable._

import java.util.{Set => javaSet}
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

}
