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

import org.apache.hadoop.hive.ql.security.authorization.PrivilegeType
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{AcidDelCommand, AcidUpdateCommand, CreateTable, CreateTempViewUsing}

object SentryAuthUtils {

  def retriveInputOutputEntities(plan: LogicalPlan): java.util.HashSet[AuthzEntity] = {
    val result: java.util.HashSet[AuthzEntity] = new java.util.HashSet[AuthzEntity]()
    plan.transformDown {
      case insertTable: InsertIntoTable =>
        val tableName = insertTable.tableName
        val dbName = if (insertTable.dbName.isDefined && insertTable.dbName.get != null) {
          insertTable.dbName.get
        } else null
        result.add(AuthzEntity(PrivilegeType.INSERT
          , tableName, dbName))
        insertTable
      case readTable: UnresolvedRelation =>
        val tableName = readTable.tableIdentifier.table
        val dbName = if (readTable.tableIdentifier.database.isDefined
          && readTable.tableIdentifier.database.get != null) {
          readTable.tableIdentifier.database.get
        } else null
        result.add(AuthzEntity(PrivilegeType.SELECT, tableName, dbName))
        readTable
      case createTable: CreateTable =>
        val tableName = createTable.tableDesc.identifier.table
        val dbName = createTable.tableDesc.identifier.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.CREATE, tableName, dbName))
        val logicalPlan: LogicalPlan = createTable.query.getOrElse {null}
        if (logicalPlan != null) {
          val createAs = retriveInputOutputEntities(logicalPlan)
          result.addAll(createAs)
        }
        createTable
      case delete: AcidDelCommand =>
        val tableName = delete.tableIdentifier.table
        val dbName = delete.tableIdentifier.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.DELETE, tableName, dbName))
        delete
      case update: AcidUpdateCommand =>
        val tableName = update.tableIdent.table
        val dbName = update.tableIdent.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.ALTER_DATA, tableName, dbName))
        update
      case createView: CreateTempViewUsing =>
        val tableName = createView.tableIdent.table
        val dbName = createView.tableIdent.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.CREATE, tableName, dbName))
        createView
      case loadData: LoadDataCommand =>
        val tableName = loadData.table.table
        val dbName = loadData.table.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.ALTER_DATA, tableName, dbName))
        loadData
      case truncateTable: TruncateTableCommand =>
        val tableName = truncateTable.tableName.table
        val dbName = truncateTable.tableName.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.DROP, tableName, dbName))
        truncateTable
      case repairTable: AlterTableRecoverPartitionsCommand =>
        val tableName = repairTable.tableName.table
        val dbName = repairTable.tableName.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName))
        repairTable
      case createDb: CreateDatabaseCommand =>
        val dbName = createDb.databaseName
        result.add(AuthzEntity(PrivilegeType.CREATE, null, dbName))
        createDb
      case alterDb: AlterDatabasePropertiesCommand =>
        val dbName = alterDb.databaseName
        result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, null, dbName))
        alterDb
      case dropDb: DropDatabaseCommand =>
        val dbName = dropDb.databaseName
        result.add(AuthzEntity(PrivilegeType.DROP, null, dbName))
        dropDb
      case dropTable: DropTableCommand =>
        val tableName = dropTable.tableName.table
        val dbName = dropTable.tableName.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.DROP, tableName, dbName))
        dropTable
      case renameTable: AlterTableRenameCommand =>
        val tableName1 = renameTable.oldName.table
        val dbName1 = renameTable.oldName.database.getOrElse {null}
        val tableName2 = renameTable.newName.table
        val dbName2 = renameTable.newName.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.DROP, tableName1, dbName1))
        result.add(AuthzEntity(PrivilegeType.CREATE, tableName2, dbName2))
        renameTable
      case atp: AlterTableSetPropertiesCommand =>
        val tableName = atp.tableName.table
        val dbName = atp.tableName.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName))
        atp
      case atup: AlterTableUnsetPropertiesCommand =>
        val tableName = atup.tableName.table
        val dbName = atup.tableName.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName))
        atup
      case atsp: AlterTableSerDePropertiesCommand =>
        val tableName = atsp.tableName.table
        val dbName = atsp.tableName.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName))
        atsp
      case addP: AlterTableAddPartitionCommand =>
        val tableName = addP.tableName.table
        val dbName = addP.tableName.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName))
        addP
      case addC: AlterTableAddColumnsCommand =>
        val tableName = addC.tableName.table
        val dbName = addC.tableName.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName))
        addC
      case atcc: AlterTableChangeColumnsCommand =>
        val tableName = atcc.tableName.table
        val dbName = atcc.tableName.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName))
        atcc
      case renameP: AlterTableRenamePartitionCommand =>
        val tableName = renameP.tableName.table
        val dbName = renameP.tableName.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName))
        renameP
      case dropP: AlterTableDropPartitionCommand =>
        val tableName = dropP.tableName.table
        val dbName = dropP.tableName.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName))
        result.add(AuthzEntity(PrivilegeType.DROP, tableName, dbName))
        dropP
      case atsl: AlterTableSetLocationCommand =>
        val tableName = atsl.tableName.table
        val dbName = atsl.tableName.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.ALTER_METADATA, tableName, dbName))
        atsl
      case createTableLike: CreateTableLikeCommand =>
        val tableName = createTableLike.targetTable.table
        val dbName = createTableLike.targetTable.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.CREATE, tableName, dbName))
        createTableLike
      case createView2: CreateViewCommand =>
        val tableName = createView2.name.table
        val dbName = createView2.name.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.CREATE, tableName, dbName))
        if (createView2.child != null) {
          val createAs = retriveInputOutputEntities(createView2.child)
          result.addAll(createAs)
        }
        createView2
      case alterView: AlterViewAsCommand =>
        val tableName = alterView.name.table
        val dbName = alterView.name.database.getOrElse {null}
        result.add(AuthzEntity(PrivilegeType.CREATE, tableName, dbName))
        if (alterView.query != null) {
          val createAs = retriveInputOutputEntities(alterView.query)
          result.addAll(createAs)
        }
        alterView
    }
    result
  }

}

case class AuthzEntity(pType: PrivilegeType, table: String, database: String)
