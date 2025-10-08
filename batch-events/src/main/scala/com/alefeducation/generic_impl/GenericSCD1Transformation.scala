package com.alefeducation.generic_impl

import com.alefeducation.base.SparkBatchService
import com.alefeducation.transformer.BaseTransform
import com.alefeducation.util.BatchTransformerUtility.OptionalDataFrameTransformer
import com.alefeducation.util.DataFrameUtility.getUTCDateFrom
import com.alefeducation.util.Resources._
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.generic_transform.CustomTransformationsUtility._
import com.typesafe.config.Config
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class GenericSCD1Transformation(session: SparkSession, service: SparkBatchService, serviceName: String) extends BaseTransform(serviceName: String) {

  override val sources: List[String] =
    getList(serviceName, "create.sources") ++
      getList(serviceName, "update.sources") ++
      getList(serviceName, "delete.sources")

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

  override def transform(data: Map[String, Option[DataFrame]], startId: Long): Option[DataFrame] = {
    import com.alefeducation.util.BatchTransformerUtility._
    import session.implicits._
    val createDataframe = filterAndUnionDataframes("create.sources", data)
    val updateDataframe = filterAndUnionDataframes("update.sources", data)
    val deleteDataframe = filterAndUnionDataframes("delete.sources", data)

    val columnMapping = getOptionalMap(serviceName, "column-mapping")
    val entity = getOptionString(serviceName, "entity").getOrElse("")
    val entityPrefix = getEntityPrefix(entity)
    val uniqueId = getList(serviceName, "unique-ids")

    val createdMapping = getMappings(columnMapping, getOptionalMap(serviceName, "create.column-mapping"))
    val updatedMapping = getMappings(columnMapping, getOptionalMap(serviceName, "update.column-mapping"))
    val deletedMapping = getMappings(columnMapping, getOptionalMap(serviceName, "delete.column-mapping"))
    val allMapping = (createdMapping ++ updatedMapping) ++ deletedMapping
    val customTransformations: Seq[Config] = getConfigList(serviceName, "transformations")

    val created =
      createDataframe
        .map(_.transformForInsertOrUpdate(createdMapping, entity, uniqueId)
          .withColumn(s"operation", lit("create"))
        )

    val updated: Option[DataFrame] =
      updateDataframe
        .map(_.transformForInsertOrUpdate(updatedMapping, entity, uniqueId)
          .withColumn(s"${entityPrefix}updated_time", $"${entityPrefix}created_time")
          .withColumn(s"${entityPrefix}dw_updated_time", $"${entityPrefix}dw_created_time")
          .withColumn(s"operation", lit("update"))
          .drop(s"${entityPrefix}dw_created_time", s"${entityPrefix}created_time")
        )

    val deleted =
      deleteDataframe
        .map(_.transformForDelete(deletedMapping, entity, uniqueId)
          .withColumn(s"operation", lit("delete"))
          .drop(s"${entityPrefix}dw_created_time", s"${entityPrefix}created_time")
        )

    val idColumns: List[String] = uniqueId.map(allMapping)
    val createdIds = created.map(_.select(idColumns.map(col): _*).distinct())
    val updatedDeletedIds = deleted.map(_.select(idColumns.map(col): _*).distinct())
      .unionOptionalByName(updated.map(_.select(idColumns.map(col): _*).distinct()))

    val dwhIds = updatedDeletedIds.map(_.dropDuplicates().exceptOptional(createdIds))

    val dataFromDWH = getOptionString(serviceName, "delta-table-path").flatMap(s => service.readOptional(s, session))
      .joinOptionalStrict(dwhIds, idColumns)
      .map(_.withColumn(s"operation", lit("create_dwh")))

    val statusColumn = "operation"

    val finalDataframe = created
      .unionOptionalByNameWithEmptyCheck(dataFromDWH, allowMissingColumns = true)
      .unionOptionalByNameWithEmptyCheck(updated, allowMissingColumns = true)
      .unionOptionalByNameWithEmptyCheck(deleted, allowMissingColumns = true)
      .map(applyCustomTransformations(customTransformations, _))
      .map(df => df.filter($"$statusColumn" === "create")
        .drop(s"${entityPrefix}dw_id")
        .genDwId(s"${entityPrefix}dw_id", startId, s"${entityPrefix}created_time")
        .unionByName(df.filter($"$statusColumn" =!= "create"), true))

    // Identify other columns dynamically
    val otherColumns: List[String] = finalDataframe.map(_.columns.filterNot(col => col == statusColumn)).toList.flatten

    // Pivot the DataFrame to have each status as a separate column set
    val pivotedDF: Option[DataFrame] = finalDataframe.map(_.groupBy(idColumns.map(col): _*).pivot(statusColumn).agg(
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
    pivotedDF.map(_.select(selectColumns: _*))
  }

  private def filterAndUnionDataframes(configName: String, data: Map[String, Option[DataFrame]]) = {
    data.filter { case (path, _) =>
      val list = getList(serviceName, configName)
      list.contains(path)
    }.values.toList.reduceOption(_ unionOptionalByNameWithEmptyCheck(_, allowMissingColumns = true)).flatten
      .map(_.withColumn("occurredOn", getUTCDateFrom(col("occurredOn"))))
  }
}

object GenericSCD1Transformation {
  def main(args: Array[String]): Unit = {
    val serviceName = args(0)
    val session: SparkSession = SparkSessionUtils.getSession(args(0))
    val service = new SparkBatchService(args(0), session)
    val transformer = new GenericSCD1Transformation(session, service, serviceName)
    transformer.run
  }
}
