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

package org.apache.spark.sql.delta.catalog

import java.net.URI
import java.util.Locale
import java.{util => ju}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalog.v2.TableChange._
import org.apache.spark.sql.catalog.v2.{Identifier, TableChange}
import org.apache.spark.sql.catalog.v2.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.delta.DeltaOperations.QualifiedColTypeWithPositionForLog
import org.apache.spark.sql.delta.{DeltaErrors, DeltaLog, DeltaOperations}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.v2.DataSourceCatalog
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

// scalastyle:off
class DeltaSessionCatalog(sparkSession: SparkSession) extends DataSourceCatalog {

  def this() = this(SparkSession.active)

  private val sessionCatalog = sparkSession.sessionState.catalog

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {}

  override def name(): String = "delta-session"

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    require(namespace.length <= 1)
    val db = namespace.headOption.getOrElse(
      sparkSession.sessionState.catalog.getCurrentDatabase)
    sessionCatalog.listTables(db).map {
      ident => Identifier.of(Array(db), ident.table)
    }.toArray
  }

  private def isDeltaTable(catalogTable: CatalogTable): Boolean = {
    isDeltaTable(catalogTable.provider)
  }

  private def isDeltaTable(ds: DataSource): Boolean = {
    classOf[DeltaDataSource].isAssignableFrom(ds.providingClass)
  }

  private def isDeltaTable(properties: ju.Map[String, String]): Boolean = {
    isDeltaTable(Option(properties.get("provider")))
  }

  private def isDeltaTable(provider: Option[String]): Boolean = {
    provider.map(_.toLowerCase(Locale.US)).contains("delta")
  }

  private implicit class V1Identifier(ident: Identifier) {
    def asTableIdentifier: TableIdentifier = {
      require(ident.namespace().length <= 1)
      TableIdentifier(ident.name(), ident.namespace().headOption)
    }
  }

  private implicit class TableAsDataSource(table: CatalogTable) {
    def asDataSource: DataSource = {
      val pathOption = table.storage.locationUri.map("path" -> CatalogUtils.URIToString(_))
      DataSource(
        sparkSession,
        // In older version(prior to 2.1) of Spark, the table schema can be empty and should be
        // inferred at runtime. We should still support it.
        userSpecifiedSchema = if (table.schema.isEmpty) None else Some(table.schema),
        partitionColumns = table.partitionColumnNames,
        bucketSpec = table.bucketSpec,
        className = table.provider.get,
        options = table.storage.properties ++ pathOption,
        catalogTable = Some(table))
    }
  }

  private def enrichTable(tableDesc: CatalogTable): CatalogTable = {
    if (isDeltaTable(tableDesc)) {
      val snapshot = DeltaLog.forTable(sparkSession, tableDesc).update()

      tableDesc.copy(
        schema = snapshot.metadata.schema,
        partitionColumnNames = snapshot.metadata.partitionColumns,
        properties = snapshot.metadata.configuration)
    } else {
      tableDesc
    }
  }

  override def loadTable(ident: Identifier): DataSource = {
    val table = sessionCatalog.getTableMetadata(ident.asTableIdentifier)
    enrichTable(table).asDataSource
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: ju.Map[String, String]): DataSource = {

    val tableType = if (properties.containsKey("location")) {
      CatalogTableType.EXTERNAL
    } else {
      CatalogTableType.MANAGED
    }

    if (isDeltaTable(properties)) {
      val metadata = setupMetadata(ident, schema, partitions, properties)
      val location = getLocation(ident, properties)

      val deltaLog = DeltaLog.forTable(SparkSession.active, new Path(location))

      val txn = deltaLog.startTransaction()
      if (txn.readVersion > -1) {
        throw new IllegalArgumentException(s"A Delta table already exists at $location")
      }
      val props = getMetaStoreProperties(properties)

      txn.commit(
        metadata :: Nil,
        DeltaOperations.CreateTable(metadata, tableType == CatalogTableType.MANAGED))

      println("made commit")

      val storage = DataSource.buildStorageFormatFromOptions(properties.asScala.toMap)
        .copy(locationUri = Option(properties.get("location")).map(CatalogUtils.stringToURI))

      val catalogTable = CatalogTable(
          identifier = ident.asTableIdentifier,
          tableType = tableType,
          storage = storage,
          schema = new StructType(),
          provider = Some("delta"),
          partitionColumnNames = metadata.partitionColumns,
          bucketSpec = None,
          properties = props.asScala.toMap,
          tracksPartitionsInCatalog = false,
          comment = Option(properties.get("comment"))
      )

      sessionCatalog.createTable(
        catalogTable,
        ignoreIfExists = false,
        validateLocation = false
      )
    } else {
      val partitionColumns = partitions.map {
        case IdentityTransform(FieldReference(Seq(col))) => col
        case t =>
          throw new UnsupportedOperationException(s"Unsupported partitioning transform: $t")
      }

      val storage = DataSource.buildStorageFormatFromOptions(properties.asScala.toMap)
        .copy(locationUri = Option(properties.get("location")).map(CatalogUtils.stringToURI))

      val catalogTable = CatalogTable(
        identifier = ident.asTableIdentifier,
        tableType = tableType,
        storage = storage,
        schema = new StructType(),
        provider = Option(properties.get("provider")),
        partitionColumnNames = partitionColumns,
        bucketSpec = None,
        properties = properties.asScala.toMap,
        tracksPartitionsInCatalog = false,
        comment = Option(properties.get("comment"))
      )

      sessionCatalog.createTable(
        catalogTable,
        ignoreIfExists = false,
        validateLocation = true
      )
    }

    loadTable(ident)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): DataSource = {
    val table = loadTable(ident).catalogTable.get
    val newProperties = new ju.HashMap[String, String](table.catalogTable.get.properties.asJava)
    val addedProps = new ju.HashMap[String, String]()
    val removedProps = new ArrayBuffer[String]()
    var operation: DeltaOperations.Operation = null
    var tableSchema = table.schema

    def setOperation(op: DeltaOperations.Operation): Unit = {
      if (operation != null) {
        throw new IllegalStateException("Unexpected")
      }
      operation = op
    }

    val resolver = sparkSession.sessionState.conf.resolver

    changes.foreach {
      case addCol: AddColumn =>
        val schema = table.schema
        val fieldPath = addCol.fieldNames().init
        val (pathToParent, lengthOfField) = SchemaUtils.findColumnPosition(
          fieldPath, schema, resolver)
        val fieldMetadata = new MetadataBuilder()
        Option(addCol.comment()).foreach(fieldMetadata.putString("comment", _))
        val field = StructField(
          addCol.fieldNames().last,
          addCol.dataType(),
          addCol.isNullable,
          fieldMetadata.build())
        tableSchema = SchemaUtils.addColumn(schema, field, pathToParent :+ lengthOfField)
        setOperation(DeltaOperations.AddColumns(Seq(
          QualifiedColTypeWithPositionForLog(fieldPath, field, None))))

      case rename: RenameColumn =>
        throw DeltaErrors.operationNotSupportedException("ALTER TABLE RENAME COLUMN")

      case update: UpdateColumnType =>
        val (oldFieldIndex, _) = SchemaUtils.findColumnPosition(
          update.fieldNames(),
          table.schema)
        val oldField = oldFieldIndex.foldLeft(table.schema.asInstanceOf[DataType]) {
          case (structType: StructType, ordinal) => structType(ordinal).dataType
          case _ => throw new IllegalStateException("Unexpected")
        }
        val canChangeType = SchemaUtils.canChangeDataType(
          oldField, update.newDataType(), sparkSession.sessionState.conf.resolver)
        canChangeType.foreach(e => throw new AnalysisException(e))
        val (droppedSchema, originalColumn) = SchemaUtils.dropColumn(table.schema, oldFieldIndex)
        val newColumn = originalColumn.copy(
          dataType = update.newDataType(),
          nullable = update.isNullable)
        tableSchema = SchemaUtils.addColumn(droppedSchema, newColumn, oldFieldIndex)
        setOperation(DeltaOperations.ChangeColumn(
          update.fieldNames().init, newColumn.name, newColumn, None))

      case update: UpdateColumnComment =>
        val (oldFieldIndex, _) = SchemaUtils.findColumnPosition(
          update.fieldNames(),
          table.schema)
        val oldField = oldFieldIndex.foldLeft(table.schema.asInstanceOf[DataType]) {
          case (structType: StructType, ordinal) => structType(ordinal).dataType
          case _ => throw new IllegalStateException("Unexpected")
        }

        val (droppedSchema, originalColumn) = SchemaUtils.dropColumn(table.schema, oldFieldIndex)
        val metaBuilder = new MetadataBuilder()
        metaBuilder.putString("comment", update.newComment())
        val newColumn = originalColumn.copy(
          metadata = metaBuilder.build())
        tableSchema = SchemaUtils.addColumn(droppedSchema, newColumn, oldFieldIndex)
        setOperation(DeltaOperations.ChangeColumn(
          update.fieldNames().init, newColumn.name, newColumn, None))

      case _: DeleteColumn =>
        throw DeltaErrors.operationNotSupportedException("ALTER TABLE DROP COLUMN")

      case set: SetProperty =>
        newProperties.put(set.property, set.value)
        addedProps.put(set.property(), set.value())

      case unset: RemoveProperty =>
        newProperties.remove(unset.property)
        removedProps += unset.property()

      case _ =>
      // ignore non-property changes
    }

    if (!addedProps.isEmpty) {
      setOperation(DeltaOperations.SetTableProperties(addedProps.asScala.toMap))
    } else if (removedProps.nonEmpty) {
      setOperation(DeltaOperations.UnsetTableProperties(removedProps, ifExists = false))
    }

    if (isDeltaTable(table)) {
      val location = table.location

      val deltaLog = DeltaLog.forTable(SparkSession.active, new Path(location))
      val txn = deltaLog.startTransaction()
      val props = newProperties.asScala.toMap
      val newMetadata = txn.metadata.copy(
        schemaString = tableSchema.json,
        configuration = props
      )
      txn.commit(newMetadata :: Nil, operation)

    } else {
      ???
    }

    loadTable(ident)
  }

  override def dropTable(ident: Identifier): Boolean = {
    sessionCatalog.dropTable(ident.asTableIdentifier, ignoreIfNotExists = false, purge = false)
    true
  }

  private def setupMetadata(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: ju.Map[String, String]): Metadata = {
    val partitionColumns = partitions.map {
      case IdentityTransform(FieldReference(Seq(col))) => col
      case t =>
        throw new UnsupportedOperationException(s"Unsupported partitioning transform: $t")
    }

    val storageProps = Set("location", "comment", "provider")
    val tableProperties = properties.asScala.filterKeys(p => !storageProps.contains(p))

    Metadata(
      name = ident.name(),
      schemaString = schema.json,
      partitionColumns = partitionColumns,
      description = properties.get("comment"),
      configuration = tableProperties.toMap
    )
  }

  private def getLocation(ident: Identifier, properties: ju.Map[String, String]): URI = {
    Option(properties.get("location")).map(CatalogUtils.stringToURI).getOrElse(
      sessionCatalog.defaultTablePath(
        TableIdentifier(ident.name(), ident.namespace().lastOption))
    )
  }

  private def getMetaStoreProperties(properties: ju.Map[String, String]): ju.Map[String, String] = {
    val newProps = new ju.HashMap[String, String]()
    Seq("location", "comment", "provider").foreach { key =>
      Option(properties.get(key)).foreach { value =>
        newProps.put(key, value)
      }
    }
    newProps
  }
}

