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
import java.{util => ju}
import java.util.Locale

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalog.v2.TableChange._
import org.apache.spark.sql.catalog.v2.{Identifier, StagingTableCatalog, TableChange}
import org.apache.spark.sql.catalog.v2.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.delta.DeltaOperations.QualifiedColTypeWithPositionForLog
import org.apache.spark.sql.delta.{DeltaConfigs, DeltaErrors, DeltaLog, DeltaOperations, OptimisticTransaction}
import org.apache.spark.sql.delta.actions.Metadata
import org.apache.spark.sql.delta.schema.SchemaUtils
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.delta.util.DeltaFileOperations
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.v2.{CatalogTableAsV2, V2SessionCatalog}
import org.apache.spark.sql.internal.SessionState
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceV1Table, DataSourceV1TableCatalog}
import org.apache.spark.sql.sources.v2.{StagedTable, Table, TableCapability}
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructField, StructType}
import org.apache.spark.util.Utils

// scalastyle:off
class DeltaSessionCatalog(sessionState: SessionState) extends V2SessionCatalog(sessionState)
  with StagingTableCatalog
  with DataSourceV1TableCatalog {

  def this() = this(SparkSession.active.sessionState)

  private def isDeltaTable(properties: ju.Map[String, String]): Boolean = {
    isDeltaTable(Option(properties.get("provider")))
  }

  private def isDeltaTable(catalogTable: CatalogTableAsV2): Boolean = {
    isDeltaTable(catalogTable.v1Table.provider)
  }

  private def isDeltaTable(provider: Option[String]): Boolean = {
    provider.map(_.toLowerCase(Locale.US)).contains("delta")
  }

  override def loadTable(ident: Identifier): Table = {
    val catalogTable = super.loadTable(ident).asInstanceOf[CatalogTableAsV2]
    if (isDeltaTable(catalogTable)) {
      return DeltaTableV2(catalogTable.v1Table.location.toString)
    }

    catalogTable
  }

  private def createCatalogTable(
      ident: Identifier,
      properties: Map[String, String]): CatalogTable = {
    val location = properties.get("location")
    val storage = DataSource.buildStorageFormatFromOptions(properties)
      .copy(locationUri = location.map(CatalogUtils.stringToURI))
    val tableType = if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED

    CatalogTable(
      identifier = ident.asTableIdentifier,
      tableType = tableType,
      storage = storage,
      schema = new StructType(),
      provider = Some("delta"),
      partitionColumnNames = Nil,
      bucketSpec = None,
      properties = properties,
      tracksPartitionsInCatalog = false,
      comment = properties.get("comment"))
  }

  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: ju.Map[String, String]): Table = {

    if (isDeltaTable(properties)) {
      val metadata = setupMetadata(ident, schema, partitions, properties)
      val location = getLocation(ident, properties)

      val deltaLog = DeltaLog.forTable(SparkSession.active, new Path(location))

      val props = getMetaStoreProperties(properties)
      val txn = deltaLog.startTransaction()
      if (txn.readVersion == -1) {
        txn.commit(
          metadata :: Nil,
          DeltaOperations.CreateTable(metadata, !properties.containsKey("location")))
      } else {
        verifyTableMetadata(txn, metadata.schema, metadata.partitionColumns, metadata.configuration)
      }

      sessionState.catalog.createTable(
        createCatalogTable(ident, props.asScala.toMap),
        ignoreIfExists = false,
        validateLocation = false)

      DeltaTableV2(location.toString)
    } else {
      super.createTable(ident, schema, partitions, properties)
    }
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val table = loadTable(ident)
    val newProperties = new ju.HashMap[String, String](table.properties())
    val addedProps = new ju.HashMap[String, String]()
    val removedProps = new ArrayBuffer[String]()
    var operation: DeltaOperations.Operation = null
    var tableSchema = table.schema()

    def setOperation(op: DeltaOperations.Operation): Unit = {
      if (operation != null) {
        throw new IllegalStateException("Unexpected")
      }
      operation = op
    }

    val resolver = sessionState.conf.resolver

    changes.foreach {
      case addCol: AddColumn =>
        val schema = table.schema()
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
          table.schema())
        val oldField = oldFieldIndex.foldLeft(table.schema().asInstanceOf[DataType]) {
          case (structType: StructType, ordinal) => structType(ordinal).dataType
          case _ => throw new IllegalStateException("Unexpected")
        }
        val canChangeType = SchemaUtils.canChangeDataType(
          oldField, update.newDataType(), sessionState.conf.resolver)
        canChangeType.foreach(e => throw new AnalysisException(e))
        val (droppedSchema, originalColumn) = SchemaUtils.dropColumn(table.schema(), oldFieldIndex)
        val newColumn = originalColumn.copy(
          dataType = update.newDataType(),
          nullable = update.isNullable)
        tableSchema = SchemaUtils.addColumn(droppedSchema, newColumn, oldFieldIndex)
        setOperation(DeltaOperations.ChangeColumn(
          update.fieldNames().init, newColumn.name, newColumn, None))

      case update: UpdateColumnComment =>
        val (oldFieldIndex, _) = SchemaUtils.findColumnPosition(
          update.fieldNames(),
          table.schema())
        val oldField = oldFieldIndex.foldLeft(table.schema().asInstanceOf[DataType]) {
          case (structType: StructType, ordinal) => structType(ordinal).dataType
          case _ => throw new IllegalStateException("Unexpected")
        }

        val (droppedSchema, originalColumn) = SchemaUtils.dropColumn(table.schema(), oldFieldIndex)
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

    table match {
      case delta: DeltaTableV2 =>
        val txn = delta.deltaLog.startTransaction()
        val props = newProperties.asScala.toMap
        val newMetadata = txn.metadata.copy(
          schemaString = tableSchema.json,
          configuration = props
        )
        txn.commit(newMetadata :: Nil, operation)

        delta
      case _ =>
        super.alterTable(ident, changes: _*)
    }
  }

  override def stageCreate(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: ju.Map[String, String]): StagedTable = {
    if (!isDeltaTable(properties)) {
      throw new IllegalArgumentException(s"Not a Delta table: $properties")
    }
    val metadata = setupMetadata(ident, schema.asNullable, partitions, properties)
    val location = getLocation(ident, properties)
    val props = getMetaStoreProperties(properties)
    val commitOp = () => sessionState.catalog.createTable(
      createCatalogTable(ident, props.asScala.toMap),
      ignoreIfExists = false,
      validateLocation = false)
    StagedDeltaTable(
      SparkSession.active, location, metadata, commitOp, clearLocationIfFailed = true)
  }

  override def stageReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: ju.Map[String, String]): StagedTable = {
    if (!isDeltaTable(properties)) {
      throw new IllegalArgumentException(s"Not a Delta table: $properties")
    }
    val metadata = setupMetadata(ident, schema.asNullable, partitions, properties)
    val location = getLocation(ident, properties)
    val props = getMetaStoreProperties(properties)
    val commitOp = () => sessionState.catalog.createTable(
      createCatalogTable(ident, props.asScala.toMap),
      ignoreIfExists = false,
      validateLocation = false)
    StagedDeltaTable(
      SparkSession.active, location, metadata, commitOp, clearLocationIfFailed = false)
  }

  override def stageCreateOrReplace(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: ju.Map[String, String]): StagedTable = {
    if (!isDeltaTable(properties)) {
      throw new IllegalArgumentException(s"Not a Delta table: $properties")
    }
    val metadata = setupMetadata(ident, schema.asNullable, partitions, properties)
    val location = getLocation(ident, properties)
    val props = getMetaStoreProperties(properties)
    val commitOp = () => sessionState.catalog.createTable(
      createCatalogTable(ident, props.asScala.toMap),
      ignoreIfExists = false,
      validateLocation = false)
    StagedDeltaTable(
      SparkSession.active, location, metadata, commitOp, clearLocationIfFailed = false)
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
    val validatedConfigurations = DeltaConfigs.validateConfigurations(tableProperties.toMap)

    Metadata(
      name = ident.name(),
      schemaString = schema.json,
      partitionColumns = partitionColumns,
      description = properties.get("comment"),
      configuration = validatedConfigurations
    )
  }

  private def getLocation(ident: Identifier, properties: ju.Map[String, String]): URI = {
    Option(properties.get("location")).map(CatalogUtils.stringToURI).getOrElse(
      sessionState.catalog.defaultTablePath(
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

  /**
   * Verify against our transaction metadata that the user specified the right metadata for the
   * table.
   */
  private def verifyTableMetadata(
      txn: OptimisticTransaction,
      schema: StructType,
      partitionColumnNames: Seq[String],
      properties: Map[String, String]): Unit = {
    val existingMetadata = txn.metadata
    val path = txn.deltaLog.dataPath

    // The delta log already exists. If they give any configuration, we'll make sure it all matches.
    // Otherwise we'll just go with the metadata already present in the log.
    // The schema compatibility checks will be made in `WriteIntoDelta` for CreateTable
    // with a query
    if (txn.readVersion > -1) {
      if (schema.nonEmpty && schema != existingMetadata.schema) {
        // We check exact alignment on create table if everything is provided
        throw new AnalysisException(
          s"""
             |The specified schema does not match the existing schema at $path.
             |
             |== Specified ==
             |${schema.treeString}
             |
             |== Existing ==
             |${existingMetadata.schema.treeString}
             """.stripMargin)
      }

      // If schema is specified, we must make sure the partitioning matches, even the partitioning
      // is not specified.
      if (schema.nonEmpty &&
        partitionColumnNames != existingMetadata.partitionColumns) {
        throw new AnalysisException(
          s"""
             |The specified partitioning does not match the existing partitioning at $path.
             |
             |== Specified ==
             |${partitionColumnNames.mkString(", ")}
             |
             |== Existing ==
             |${existingMetadata.partitionColumns.mkString(", ")}
             """.stripMargin)
      }

      if (properties.nonEmpty && properties != existingMetadata.configuration) {
        throw new AnalysisException(
          s"""
             |The specified properties do not match the existing properties at $path.
             |
             |== Specified ==
             |${properties.map { case (k, v) => s"$k=$v" }.mkString("\n")}
             |
             |== Existing ==
             |${existingMetadata.configuration.map { case (k, v) => s"$k=$v" }.mkString("\n")}
             """.stripMargin)
      }
    }
  }
}

trait DeltaV2TableMixin extends Table {
  private[catalog] val deltaLog: DeltaLog

  protected val metadata: Metadata

  override def name(): String = "Delta Lake"

  override def schema(): StructType = metadata.schema

  override def partitioning(): Array[Transform] = {
    metadata.partitionColumns.map(col =>
      IdentityTransform(FieldReference(Seq(col)))).toArray
  }

  override def properties(): ju.Map[String, String] = {
    metadata.configuration.asJava
  }

  override def capabilities(): ju.Set[TableCapability] =
    new ju.HashSet[TableCapability](ju.Collections.singleton(TableCapability.ACCEPT_ANY_SCHEMA))
}

case class DeltaTableV2(location: String) extends DataSourceV1Table with DeltaV2TableMixin {

  private lazy val spark: SparkSession = SparkSession.active

  override val v1Relation: BaseRelation =
    new DeltaDataSource().createRelation(spark.sqlContext, Map("path" -> location))

  override private[catalog] val deltaLog: DeltaLog = DeltaLog.forTable(spark, location)

  override protected val metadata: Metadata = deltaLog.update().metadata
}

case class StagedDeltaTable private (
    deltaLog: DeltaLog,
    commitOperation: () => Unit,
    abortOperation: () => Unit) extends StagedTable
  with DeltaV2TableMixin
  with DataSourceV1Table {

  private lazy val spark: SparkSession = SparkSession.active

  override lazy val v1Relation: BaseRelation = new DeltaDataSource().createRelation(
    spark.sqlContext, Map("path" -> deltaLog.dataPath.toString))

  override protected val metadata: Metadata = OptimisticTransaction.getActive().get.metadata

  override def commitStagedChanges(): Unit = {
    assert(OptimisticTransaction.getActive().isDefined)
    OptimisticTransaction.clearActive()
    commitOperation()
  }

  override def abortStagedChanges(): Unit = {
    assert(OptimisticTransaction.getActive().isDefined)
    OptimisticTransaction.clearActive()
    abortOperation()
  }
}

object StagedDeltaTable {
  def apply(
      spark: SparkSession,
      location: URI,
      metadata: Metadata,
      commitOperation: () => Unit,
      clearLocationIfFailed: Boolean): StagedDeltaTable = {
    val deltaLog = DeltaLog.forTable(spark, new Path(location))
    val txn = deltaLog.startTransaction()
    OptimisticTransaction.setActive(txn)
    txn.updateMetadata(metadata)

    def abort(): Unit = {
      if (clearLocationIfFailed) {

      }
    }

    new StagedDeltaTable(deltaLog, commitOperation, abort)
  }
}
