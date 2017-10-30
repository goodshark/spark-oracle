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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}

object SentryAuthUtils {

  def retriveInputOutputEntities(plan: LogicalPlan): java.util.HashSet[AuthzEntity] = {
    val result: java.util.HashSet[AuthzEntity] = new java.util.HashSet[AuthzEntity]()
    plan.transformDown {
      case insertTable: InsertIntoTable =>
        val tableName = insertTable.tableName
        val dbName = insertTable.dbName
        result.add(AuthzEntity(HiveSentryAuthzProvider.INSERT
          , new TableIdentifier(tableName, dbName)))
        insertTable
      case readTable: UnresolvedRelation =>
        result.add(AuthzEntity(HiveSentryAuthzProvider.SELECT, readTable.tableIdentifier))
        readTable
    }
    result
  }

}

case class AuthzEntity(op: String, table: TableIdentifier)
