package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.collection.immutable.Map

class ClassCategoryDimension(override val session: SparkSession) extends SparkBatchService {

  import ClassCategoryDimension._

  override val name: String = ClassCategoryDimensionName

  import session.implicits._

  override def transform(): List[Sink] = {
    val classCategoryEvents: Option[DataFrame] = readOptional(ClassCategorySourceParquet, session, isMandatory = false)
    val parquetSink = classCategoryEvents.map(_.toParquetSink(ClassCategorySinkParquet))

    val classCategorySource = addDefaultColumn(classCategoryEvents)

    val deletedTimeColumn = s"${ColumnNamePrefix}_deleted_time"
    val createdEvents = classCategorySource.flatMap(
      _.filter($"eventType" === ClassCategoryCreatedEvent)
        .transformForInsertDim(SourceToSinkColumnMappings, ColumnNamePrefix, ids = List("classCategoryId"))
        .drop(deletedTimeColumn)
        .withColumn(s"${ColumnNamePrefix}_status" ,  lit(1))
        .checkEmptyDf
    )
    val updatedEvents = classCategorySource.flatMap(
      _.filter($"eventType" === ClassCategoryUpdatedEvent)
        .transformForUpdateDim(SourceToSinkColumnMappings, ColumnNamePrefix, ids = List("classCategoryId"))
        .drop(deletedTimeColumn)
        .withColumn(s"${ColumnNamePrefix}_status" ,  lit(1))
        .checkEmptyDf
    )
    val deletedEvents = classCategorySource.flatMap(
      _.filter($"eventType" === ClassCategoryDeletedEvent)
        .transformForDelete(ClassCategoryDeletedColumnMappings, ColumnNamePrefix, idColumns = List("classCategoryId"))
        .drop(deletedTimeColumn)
        .checkEmptyDf
    )

    val redshiftCreatedSink = createdEvents
      .map(_.toRedshiftInsertSink(ClassCategoryCreatedSinkRedshift, ClassCategoryCreatedEvent))
    val redshiftUpdatedSink = updatedEvents
      .map(_.toRedshiftUpdateSink(ClassCategoryUpdatedSinkRedshift, ClassCategoryUpdatedEvent,
        ColumnNamePrefix, List(s"${ColumnNamePrefix}_id")))
    val redshiftDeletedSink = deletedEvents
      .map(_.toRedshiftUpdateSink(ClassCategoryDeletedSinkRedshift, ClassCategoryDeletedEvent,
        ColumnNamePrefix, List(s"${ColumnNamePrefix}_id")))

    val deltaCreateSink = createdEvents.flatMap(_.toCreate().map(_.toSink(ClassCategoryCreatedSinkDelta)))
    val deltaUpdatedSink = updatedEvents.flatMap(_.toUpdate().map(_.toSink(ClassCategoryUpdatedSinkDelta, ColumnNamePrefix)))
    val deltaDeletedSink = deletedEvents.flatMap(_.toUpdate().map(_.toSink(ClassCategoryDeletedSinkDelta, ColumnNamePrefix)))

    (parquetSink
      ++ redshiftCreatedSink ++ redshiftUpdatedSink ++ redshiftDeletedSink
      ++ deltaCreateSink ++ deltaUpdatedSink ++ deltaDeletedSink
      ).toList
  }

  def addDefaultColumn(df: Option[DataFrame]): Option[DataFrame] = {
    df.map(events =>
      if (events.columns.contains("organisationGlobal")) events
      else events.withColumn("organisationGlobal", lit(null).cast(StringType))
    )
  }
}

object ClassCategoryDimension {
  val ClassCategoryDimensionName = "class-category-dimension"

  // Source
  val ClassCategorySourceParquet = "parquet-class-category-source"

  // Sinks
  val ClassCategorySinkParquet = "parquet-class-category-sink"

  val ClassCategoryCreatedSinkRedshift = "redshift-class-category-created-sink"
  val ClassCategoryUpdatedSinkRedshift = "redshift-class-category-updated-sink"
  val ClassCategoryDeletedSinkRedshift = "redshift-class-category-deleted-sink"

  val ClassCategoryCreatedSinkDelta = "delta-class-category-created-sink"
  val ClassCategoryUpdatedSinkDelta = "delta-class-category-updated-sink"
  val ClassCategoryDeletedSinkDelta = "delta-class-category-deleted-sink"

  // Schemas
  val ColumnNamePrefix = "class_category"
  val SourceToSinkColumnMappings: Map[String, String] = Map(
    "classCategoryId" -> s"${ColumnNamePrefix}_id",
    "name" -> s"${ColumnNamePrefix}_name",
    "organisationGlobal" -> s"${ColumnNamePrefix}_organization_code",
    "occurredOn" -> "occurredOn"
  )
  val ClassCategoryDeletedColumnMappings: Map[String, String] = SourceToSinkColumnMappings ++
    Map(s"${ColumnNamePrefix}_status" -> s"${ColumnNamePrefix}_status")

  def main(args: Array[String]): Unit = {
    new ClassCategoryDimension(SparkSessionUtils.getSession(ClassCategoryDimensionName)).run
  }
}

