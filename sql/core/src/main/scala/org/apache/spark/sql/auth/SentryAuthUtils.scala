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

/*
  * Created by chenfolin on 2017/10/23.
  */

package org.apache.spark.sql.auth

import java.util

import org.apache.hadoop.hive.ql.security.authorization.PrivilegeType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAttribute, UnresolvedRelation, UnresolvedStar}
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{AcidDelCommand, AcidUpdateCommand, CreateTable, CreateTempViewUsing}

object SentryAuthUtils {

  def retriveInputOutputEntities(plan: LogicalPlan,
                                 sparkSession: SparkSession,
                                 cteRs: java.util.HashSet[String] =
                                 new util.HashSet[String]()): java.util.HashSet[AuthzEntity] = {
    val result: java.util.HashSet[AuthzEntity] = new java.util.HashSet[AuthzEntity]()
    var currentProject: Project = null
    var currentAggregate: Aggregate = null
    if (plan.isInstanceOf[Project]) {
      currentProject = plan.asInstanceOf[Project]
    }
    if (plan.isInstanceOf[Aggregate]) {
      currentAggregate = plan.asInstanceOf[Aggregate]
    }
    plan.transformDown {
      case subqueryFilter: Filter =>
        subqueryFilter.condition.transformDown{
          case subq: ScalarSubquery =>
            result.addAll(retriveInputOutputEntities(subq.plan, sparkSession, cteRs))
            subq
        }
        subqueryFilter
      case withas: With =>
        withas.cteRelations.foreach(cte => {
          cteRs.add(cte._1)
        })
        withas.cteRelations.foreach(cte => {
          val createAs = retriveInputOutputEntities(cte._2, sparkSession, cteRs)
          result.addAll(createAs)
        })
        withas
      case project: Project =>
        currentProject = project
        project
      case agg: Aggregate =>
        currentAggregate = agg
        agg
      case insertTable: InsertIntoTable =>
        val tableName = insertTable.tableName
        val dbName = if (insertTable.dbName.isDefined && insertTable.dbName.get != null) {
          insertTable.dbName.get
        } else null
        if (!sparkSession.isTemporaryTable(TableIdentifier.apply(
          insertTable.tableName, insertTable.dbName))) {
          result.add(AuthzEntity(PrivilegeType.INSERT
            , tableName, dbName, null))
        }
        insertTable
      case readTable: UnresolvedRelation =>
        val tableName = readTable.tableIdentifier.table
        val dbName = if (readTable.tableIdentifier.database.isDefined
          && readTable.tableIdentifier.database.get != null) {
          readTable.tableIdentifier.database.get
        } else null
        if (!(!readTable.tableIdentifier.database.isDefined && cteRs.contains(tableName))) {
          if (currentProject != null || currentAggregate != null) {
            if (!sparkSession.isTemporaryTable(readTable.tableIdentifier)) {
              val alis = readTable.alias.getOrElse(null)
              if (currentAggregate != null) {
                val columns = retriveInputEntities(currentAggregate.aggregateExpressions.filter(f => {
                  f match {
                    case attr: UnresolvedStar =>
                      if (None.equals(attr.target)) {
                        true
                      } else false
                    case _ => true
                  }
                }),
                  alis, readTable.tableName, sparkSession, readTable.tableIdentifier)
                val ir = columns.iterator()
                while (ir.hasNext) {
                  result.add(AuthzEntity(PrivilegeType.SELECT, tableName, dbName, ir.next()))
                }
                val columns2 = retriveInputEntities(currentAggregate.groupingExpressions.filter(f => {
                  f match {
                    case attr: UnresolvedStar =>
                      if (None.equals(attr.target)) {
                        true
                      } else false
                    case _ => true
                  }
                }),
                  alis, readTable.tableName, sparkSession, readTable.tableIdentifier)
                val ir3 = columns2.iterator()
                while (ir3.hasNext) {
                  result.add(AuthzEntity(PrivilegeType.SELECT, tableName, dbName, ir3.next()))
                }
                val dd = retriveInputEntities(currentAggregate.aggregateExpressions.filter(f => {
                  f match {
                    case attr: UnresolvedStar =>
                      if (None.equals(attr.target)) {
                        false
                      } else true
                    case _ => false
                  }
                }),
                  sparkSession, cteRs, readTable.tableIdentifier, alis)
                val filters: util.HashSet[String] = retriveInputEntities(currentAggregate,
                  alis, readTable.tableName, sparkSession, readTable.tableIdentifier)
                val ir2 = filters.iterator()
                while (ir2.hasNext) {
                  result.add(AuthzEntity(PrivilegeType.SELECT, tableName, dbName, ir2.next()))
                }
                if ((columns.size() + dd.size() + filters.size() + columns2.size()) == 0) {
                  result.add(AuthzEntity(PrivilegeType.SELECT, tableName, dbName, null))
                }
                result.addAll(dd)
              } else {
                val columns = retriveInputEntities(currentProject.projectList.filter(f => {
                  f match {
                    case attr: UnresolvedStar =>
                      if (None.equals(attr.target)) {
                        true
                      } else false
                    case _ => true
                  }
                }),
                  alis, readTable.tableName, sparkSession, readTable.tableIdentifier)
                val ir = columns.iterator()
                while (ir.hasNext) {
                  result.add(AuthzEntity(PrivilegeType.SELECT, tableName, dbName, ir.next()))
                }
                val dd = retriveInputEntities(currentProject.projectList.filter(f => {
                  f match {
                    case attr: UnresolvedStar =>
                      if (None.equals(attr.target)) {
                        false
                      } else true
                    case _ => false
                  }
                }),
                  sparkSession, cteRs, readTable.tableIdentifier, alis)
                val filters: util.HashSet[String] = {
                  if (currentProject != null) {
                    retriveInputEntities(currentProject, alis, readTable.tableName, sparkSession, readTable.tableIdentifier)
                  } else {
                    new util.HashSet[String]()
                  }
                }
                val ir2 = filters.iterator()
                while (ir2.hasNext) {
                  result.add(AuthzEntity(PrivilegeType.SELECT, tableName, dbName, ir2.next()))
                }
                if ((columns.size() + dd.size() + filters.size()) == 0) {
                  result.add(AuthzEntity(PrivilegeType.SELECT, tableName, dbName, null))
                }
                result.addAll(dd)
              }
            }
          }
        }
        readTable
      case createTable: CreateTable =>
        val tableName = createTable.tableDesc.identifier.table
        val dbName = createTable.tableDesc.identifier.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.CREATE, tableName, dbName, null))
        val logicalPlan: LogicalPlan = createTable.query.getOrElse {null}
        if (logicalPlan != null) {
          val createAs = retriveInputOutputEntities(logicalPlan, sparkSession, cteRs)
          result.addAll(createAs)
        }
        createTable
      case delete: AcidDelCommand =>
        val tableName = delete.tableIdentifier.table
        val dbName = delete.tableIdentifier.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(delete.tableIdentifier)) {
          result.add(AuthzEntity(PrivilegeType.DELETE, tableName, dbName, null))
        }
        delete
      case update: AcidUpdateCommand =>
        val tableName = update.tableIdent.table
        val dbName = update.tableIdent.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(update.tableIdent)) {
          result.add(AuthzEntity(PrivilegeType.ALTER_DATA, tableName, dbName, null))
        }
        update
      case createView: CreateTempViewUsing =>
        val tableName = createView.tableIdent.table
        val dbName = createView.tableIdent.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.CREATE, tableName, dbName, null))
        createView
      case loadData: LoadDataCommand =>
        val tableName = loadData.table.table
        val dbName = loadData.table.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(loadData.table)) {
          result.add(AuthzEntity(PrivilegeType.ALTER_DATA, tableName, dbName, null))
        }
        loadData
      case truncateTable: TruncateTableCommand =>
        val tableName = truncateTable.tableName.table
        val dbName = truncateTable.tableName.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(truncateTable.tableName)) {
          result.add(AuthzEntity(PrivilegeType.DROP, tableName, dbName, null))
        }
        truncateTable
      case repairTable: AlterTableRecoverPartitionsCommand =>
        val tableName = repairTable.tableName.table
        val dbName = repairTable.tableName.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(repairTable.tableName)) {
          result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName, null))
        }
        repairTable
      case createDb: CreateDatabaseCommand =>
        val dbName = createDb.databaseName
        result.add(AuthzEntity(PrivilegeType.CREATE, null, dbName, null))
        createDb
      case alterDb: AlterDatabasePropertiesCommand =>
        val dbName = alterDb.databaseName
        result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, null, dbName, null))
        alterDb
      case dropDb: DropDatabaseCommand =>
        val dbName = dropDb.databaseName
        result.add(AuthzEntity(PrivilegeType.DROP, null, dbName, null))
        dropDb
      case dropTable: DropTableCommand =>
        val tableName = dropTable.tableName.table
        val dbName = dropTable.tableName.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(dropTable.tableName)) {
          result.add(AuthzEntity(PrivilegeType.DROP, tableName, dbName, null))
        }
        dropTable
      case renameTable: AlterTableRenameCommand =>
        val tableName1 = renameTable.oldName.table
        val dbName1 = renameTable.oldName.database.getOrElse {null}
        val tableName2 = renameTable.newName.table
        val dbName2 = renameTable.newName.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(renameTable.oldName)) {
          result.add(AuthzEntity(PrivilegeType.DROP, tableName1, dbName1, null))
          result.add(AuthzEntity(PrivilegeType.CREATE, tableName2, dbName2, null))
        }
        renameTable
      case atp: AlterTableSetPropertiesCommand =>
        val tableName = atp.tableName.table
        val dbName = atp.tableName.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(atp.tableName)) {
          result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName, null))
        }
        atp
      case atup: AlterTableUnsetPropertiesCommand =>
        val tableName = atup.tableName.table
        val dbName = atup.tableName.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(atup.tableName)) {
          result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName, null))
        }
        atup
      case atsp: AlterTableSerDePropertiesCommand =>
        val tableName = atsp.tableName.table
        val dbName = atsp.tableName.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(atsp.tableName)) {
          result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName, null))
        }
        atsp
      case addP: AlterTableAddPartitionCommand =>
        val tableName = addP.tableName.table
        val dbName = addP.tableName.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(addP.tableName)) {
          result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName, null))
        }
        addP
      case addC: AlterTableAddColumnsCommand =>
        val tableName = addC.tableName.table
        val dbName = addC.tableName.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(addC.tableName)) {
          result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName, null))
        }
        addC
      case atcc: AlterTableChangeColumnsCommand =>
        val tableName = atcc.tableName.table
        val dbName = atcc.tableName.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(atcc.tableName)) {
          result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName, null))
        }
        atcc
      case renameP: AlterTableRenamePartitionCommand =>
        val tableName = renameP.tableName.table
        val dbName = renameP.tableName.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(renameP.tableName)) {
          result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName, null))
        }
        renameP
      case dropP: AlterTableDropPartitionCommand =>
        val tableName = dropP.tableName.table
        val dbName = dropP.tableName.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(dropP.tableName)) {
          result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName, null))
          result.add(AuthzEntity(PrivilegeType.DROP, tableName, dbName, null))
        }
        dropP
      case atsl: AlterTableSetLocationCommand =>
        val tableName = atsl.tableName.table
        val dbName = atsl.tableName.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(atsl.tableName)) {
          result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName, null))
        }
        atsl
      case createTableLike: CreateTableLikeCommand =>
        val tableName = createTableLike.targetTable.table
        val dbName = createTableLike.targetTable.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.CREATE, tableName, dbName, null))
        createTableLike
      case createView2: CreateViewCommand =>
        val tableName = createView2.name.table
        val dbName = createView2.name.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.CREATE, tableName, dbName, null))
        if (createView2.child != null) {
          val createAs = retriveInputOutputEntities(createView2.child, sparkSession, cteRs)
          result.addAll(createAs)
        }
        createView2
      case alterView: AlterViewAsCommand =>
        val tableName = alterView.name.table
        val dbName = alterView.name.database.getOrElse {null}
        if (!sparkSession.isTemporaryTable(alterView.name)) {
          result.add(AuthzEntity(PrivilegeType.CREATE, tableName, dbName, null))
          if (alterView.query != null) {
            val createAs = retriveInputOutputEntities(alterView.query, sparkSession, cteRs)
            result.addAll(createAs)
          }
        }
        alterView
    }
    result
  }

  def retriveInputEntities(plan: LogicalPlan, alis: String, tableName: String,
                           sparkSession: SparkSession, tableIdent: TableIdentifier
                          ): java.util.HashSet[String] = {
    val result: java.util.HashSet[String] = new java.util.HashSet[String]()
    plan.transformDown{
      case filter: Filter =>
        result.addAll(retriveInputEntities(Seq(filter.condition), alis, tableName, sparkSession, tableIdent))
        filter
    }
    result
  }

  def retriveInputEntities(plans: Seq[Expression], alis: String, tableName: String,
                           sparkSession: SparkSession, tableIdent: TableIdentifier
                          ): java.util.HashSet[String] = {
    val result: java.util.HashSet[String] = new java.util.HashSet[String]()
    plans.foreach(plan => {
      plan.transformDown{
        case attr: UnresolvedAttribute =>
          if (attr.nameParts.size == 2) {
            if ((attr.nameParts)(0).equals(alis) || (attr.nameParts)(0).equals(tableName)
              || (attr.nameParts)(0).equals(tableIdent.table)) {
              result.add((attr.nameParts)(1))
            }
          } else if (attr.nameParts.size == 1) {
            val columns: Seq[String] = sparkSession.getColumnForSelectStar(tableIdent)
            if (columns.contains((attr.nameParts)(0))) {
              result.add((attr.nameParts)(0))
            }
          }
          attr
        case attr: UnresolvedStar =>
          val columns: Seq[String] = sparkSession.getColumnForSelectStar(tableIdent)
          columns.foreach(result.add)
          attr
      }
    })
    result
  }

  def retriveInputEntities(plans: Seq[NamedExpression],
                           sparkSession: SparkSession,
                           cteRs: java.util.HashSet[String],
                           table: TableIdentifier,
                           alis: String): java.util.HashSet[AuthzEntity] = {
    val result: java.util.HashSet[AuthzEntity] = new java.util.HashSet[AuthzEntity]()
    plans.foreach(plan => {
      plan.transformDown{
        case attr: UnresolvedAttribute =>
          attr
        case attr: UnresolvedStar =>
          val dt: (String, String) = {
            if (attr.target.get.size == 2) {
              (attr.target.get(0), attr.target.get(1))
            } else {
              (sparkSession.sessionState.catalog.getCurrentDatabase, attr.target.get(0))
            }
          }
          if (!cteRs.contains(dt._2)) {
            val tableN = if (alis == null) {
              dt._2
            } else {
              if (alis.equals(dt._2)) {
                table.table
              } else {
                null
              }
            }
            if (tableN != null && table.table.equals(tableN)) {
              val columns: Seq[String] = sparkSession.getColumnForSelectStar(table)
              val ir = columns.iterator
              while (ir.hasNext) {
                result.add(AuthzEntity(PrivilegeType.SELECT, tableN, table.database.getOrElse(null), ir.next()))
              }
              if (columns.size == 0) {
                result.add(AuthzEntity(PrivilegeType.SELECT, tableN, table.database.getOrElse(null), null))
              }
            }
          }
          attr
      }
    })
    result
  }

}

case class AuthzEntity(pType: PrivilegeType, table: String, database: String, column: String)
