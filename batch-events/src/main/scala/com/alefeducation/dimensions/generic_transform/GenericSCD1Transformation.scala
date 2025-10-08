package com.alefeducation.dimensions.generic_transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources._
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.generic_transform.CustomTransformationsUtility._
import com.typesafe.config.Config
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class GenericSCD1Transformation(val session: SparkSession, val service: SparkBatchService, val serviceName: String) {

  private def getMappings(commonMapping: Option[Map[String, String]], specificMapping: Option[Map[String, String]]): Map[String, String] = {
    commonMapping match {
      case Some(extractMapping) => extractMapping
      case None =>
        specificMapping match {
          case Some(extractSpecificMapping) => extractSpecificMapping
          case None => Map.empty
        }
    }
  }

  def transform(): Option[Sink] = {
    import com.alefeducation.util.BatchTransformerUtility._
    import session.implicits._

    val startId = service.getStartIdUpdateStatus(getNestedString(serviceName, "key"))

    val createDataframe: Option[DataFrame] =
      getList(serviceName, "create.source")
        .map(s => service.readOptional(s, session, extraProps = List(("mergeSchema", "true"))))
        .reduceOption(_ unionOptionalByNameWithEmptyCheck(_, allowMissingColumns = true)).flatten
    val updateDataframe: Option[DataFrame] =
      getList(serviceName, "update.source")
        .map(s => service.readOptional(s, session, extraProps = List(("mergeSchema", "true"))))
        .reduceOption(_ unionOptionalByNameWithEmptyCheck(_, allowMissingColumns = true)).flatten
    val deleteDataframe: Option[DataFrame] =
      getList(serviceName, "delete.source")
        .map(s => service.readOptional(s, session, extraProps = List(("mergeSchema", "true"))))
        .reduceOption(_ unionOptionalByNameWithEmptyCheck(_, allowMissingColumns = true)).flatten

    val columnMapping = getOptionalMap(serviceName, "column-mapping")
    val entityName = getNestedString(serviceName, "entity")
    val uniqueId = getList(serviceName, "unique-ids")
    val sinkName = getString(serviceName, "sink")

    val createdMapping = getMappings(columnMapping, getOptionalMap(serviceName, "create.column-mapping"))
    val updatedMapping = getMappings(columnMapping, getOptionalMap(serviceName, "update.column-mapping"))
    val deletedMapping = getMappings(columnMapping, getOptionalMap(serviceName, "delete.column-mapping"))
    val allMapping = (createdMapping ++ updatedMapping) ++ deletedMapping
    val customTransformations: Seq[Config] = getConfigList(serviceName, "transformations")

    val created =
      createDataframe
        .map(_.transformForInsertOrUpdate(createdMapping, entityName, uniqueId)
          .withColumn(s"operation", lit("create"))
        )

    val updated: Option[DataFrame] =
      updateDataframe
        .map(_.transformForInsertOrUpdate(updatedMapping, entityName, uniqueId)
          .withColumn(s"${entityName}_updated_time", $"${entityName}_created_time")
          .withColumn(s"${entityName}_dw_updated_time", $"${entityName}_dw_created_time")
          .withColumn(s"operation", lit("update"))
          .drop(s"${entityName}_dw_created_time", s"${entityName}_created_time")
        )

    val deleted =
      deleteDataframe
        .map(_.transformForDelete(deletedMapping, entityName, uniqueId)
          .withColumn(s"operation", lit("delete"))
          .drop(s"${entityName}_dw_created_time", s"${entityName}_created_time")
        )

    val idColumns: List[String] = uniqueId.map(allMapping)
    val createdIds = created.map(_.select(idColumns.map(col): _*).distinct())
    val updatedDeletedIds = deleted.map(_.select(idColumns.map(col): _*).distinct())
      .unionOptionalByName(updated.map(_.select(idColumns.map(col): _*).distinct()))

    val dwhIds = updatedDeletedIds.map(_.dropDuplicates().exceptOptional(createdIds))

    val dataFromDWH = getOptionString(serviceName, "table-name").flatMap(s => service.readOptional(s, session))
      .joinOptionalStrict(dwhIds, idColumns)
      .map(_.withColumn(s"operation", lit("create")))

    val statusColumn = "operation"

    val data = created
      .unionOptionalByNameWithEmptyCheck(dataFromDWH, allowMissingColumns = true)
      .unionOptionalByNameWithEmptyCheck(updated, allowMissingColumns = true)
      .unionOptionalByNameWithEmptyCheck(deleted, allowMissingColumns = true)
      .map(applyCustomTransformations(customTransformations, _))
      .map(df => df.filter($"$statusColumn" === "create")
        .drop(s"${entityName}_dw_id")
        .genDwId(s"${entityName}_dw_id", startId, s"${entityName}_created_time")
        .unionByName(df.filter($"$statusColumn" =!= "create"), true))

    // Identify other columns dynamically
    val otherColumns: List[String] = data.map(_.columns.filterNot(col => col == statusColumn)).toList.flatten

    // Pivot the DataFrame to have each status as a separate column set
    val pivotedDF: Option[DataFrame] = data.map(_.groupBy(idColumns.map(col): _*).pivot(statusColumn).agg(
      otherColumns.map(colName => first(col(colName), ignoreNulls = true).as(colName)).head,
      otherColumns.map(colName => first(col(colName), ignoreNulls = true).as(colName)).tail: _*
    ))

    // Generate the final select clause using coalesce for each column
    val selectColumns: List[Column] = otherColumns.map { colName =>
      val createCol = pivotedDF.flatMap(df => df.columns.find(_ == s"create_$colName").map(_ => col(s"create_$colName")))
      val createDWHCol = pivotedDF.flatMap(df => df.columns.find(_ == s"create_dwh_$colName").map(_ => col(s"create_dwh_$colName")))
      val updateCol = pivotedDF.flatMap(df => df.columns.find(_ == s"update_$colName").map(_ => col(s"update_$colName")))
      val deleteCol = pivotedDF.flatMap(df => df.columns.find(_ == s"delete_$colName").map(_ => col(s"delete_$colName")))

      // Coalesce only with existing columns
      val columnsToCoalesce = Seq(deleteCol, updateCol, createCol, createDWHCol).flatten // Flatten removes Nones
      coalesce(columnsToCoalesce: _*).as(colName)
    }

    // Final select statement
    val finalDF = pivotedDF.map(_.select(selectColumns: _*).filter($"${entityName}_created_time".isNotNull))

    finalDF
      .map(
        DataSink(
          sinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> getNestedString(serviceName, "key"))
        )
      )
  }
}

object GenericSCD1Transformation {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSessionUtils.getSession(args(0))
    val service = new SparkBatchService(args(0), session)
    val transformer = new GenericSCD1Transformation(session, service, args(0))
    service.run(transformer.transform())
  }
}
