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

package org.apache.spark.sql.delta.analysis

import scala.collection.JavaConverters._

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Expression, Literal, UpCast}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{FilterExec, ProjectExec}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.util.SchemaUtils

// scalastyle:off
case object DeltaAnalysis extends Rule[LogicalPlan] {

  private def needsSchemaAdjustment(query: LogicalPlan, schema: StructType): Boolean = {
    val output = query.output
    if (output.length != schema.length) {
      // Leave it to WriteToDelta
      return false
    }
    output.map(_.name) != schema.map(_.name) ||
      !DataType.equalsIgnoreCaseAndNullability(output.toStructType, schema)
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    // INSERT INTO by ordinal
    case a @ AppendData(DataSourceV2Relation(d: DeltaTableV2, _, _), query, false)
        if query.resolved && needsSchemaAdjustment(query, d.schema()) =>
      val projection = normalizeQueryColumns(query, d)
      if (projection != query) {
        a.copy(query = projection)
      } else {
        a
      }

    case PhysicalOperation(projects, filters,
        DataSourceV2Relation(d: DeltaTableV2, output, options)) =>
      val relation = d.createRelation(
        SparkSession.active.sqlContext,
        options.asCaseSensitiveMap().asScala.toMap)
      Project(projects,
        Filter(filters.foldLeft(Literal(true).asInstanceOf[Expression])(And),
          LogicalRelation(relation, output, None, isStreaming = false)
        )
      )
  }

  private def normalizeQueryColumns(query: LogicalPlan, target: DeltaTableV2): LogicalPlan = {
    val targetAttrs = target.schema()
    // always add an UpCast. it will be removed in the optimizer if it is unnecessary.
    // TODO: support nested fields
    val project = query.output.zipWithIndex.map { case (attr, i) =>
      if (i < targetAttrs.length) {
        val targetAttr = targetAttrs(i)
        Alias(
          UpCast(attr, targetAttr.dataType),
          targetAttr.name)(explicitMetadata = Option(targetAttr.metadata))
      } else {
        attr
      }
    }
    Project(project, query)
  }
}
