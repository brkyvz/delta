/*
 * Copyright 2019 Databricks, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import java.io.File

import scala.collection.JavaConverters._

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalog.v2.Identifier
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.delta.catalog.DeltaSessionCatalog
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class DeltaDDLSuite extends QueryTest with SharedSparkSession {

  override def afterAll(): Unit = {
    val db = spark.sessionState.catalog.getCurrentDatabase
    Utils.deleteRecursively(new File(spark.sessionState.catalog.getDefaultDBPath(db)))
  }

  private def ddlTest(testName: String)(f: String => Unit): Unit = {
    test(testName) {
      spark.conf.set("spark.sql.catalog.session", classOf[DeltaSessionCatalog].getName)
      spark.conf.set("spark.databricks.delta.snapshotPartitions", 1)
      val tblName = "test_table"
      withTable(tblName) {
        f(tblName)

        val table = getTable(tblName)
        assert(table.schema.isEmpty)
        val props = new java.util.HashMap[String, String](table.properties.asJava)
        Seq("provider", "location", "comment").foreach(props.remove)
        assert(props.isEmpty, s"Properties were not empty in the metastore: $props")
        assert(table.partitionColumnNames.isEmpty)
      }
    }
  }

  private def getTable(tblName: String): CatalogTable = {
    spark.sessionState.catalog.getTableMetadata(TableIdentifier(tblName))
  }

  ddlTest("CREATE TABLE") { tblName =>
    sql(s"""CREATE TABLE $tblName (id bigint) USING delta""")
  }

  ddlTest("CREATE TABLE AS SELECT") { tblName =>
    sql(
      s"""CREATE TABLE $tblName USING delta PARTITIONED BY (part)
         |AS SELECT id, id % 5 as part FROM RANGE(10)
       """.stripMargin
    )
  }

  ddlTest("REPLACE TABLE AS SELECT") { tblName =>
    sql(s"""CREATE OR REPLACE TABLE $tblName (id bigint) USING delta""")
  }

  ddlTest("ALTER TABLE SET TBLPROPERTIES") { tblName =>
    sql(s"""CREATE TABLE $tblName (id bigint) USING delta""")
    sql(s"""ALTER TABLE $tblName SET TBLPROPERTIES (delta.appendOnly = true)""")
    // val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tblName))
  }

  ddlTest("ALTER TABLE UNSET TBLPROPERTIES") { tblName =>
    sql(s"CREATE TABLE $tblName (id bigint) USING delta TBLPROPERTIES (delta.appendOnly = true)")
    sql(s"""ALTER TABLE $tblName UNSET TBLPROPERTIES (delta.appendOnly)""")
    // val deltaLog = DeltaLog.forTable(spark, TableIdentifier(tblName))
  }

  ddlTest("ALTER TABLE ADD COLUMN") { tblName =>
    sql(s"""CREATE TABLE $tblName (id bigint) USING delta""")
    sql(s"""ALTER TABLE $tblName ADD COLUMNS (part bigint)""")
  }
}
