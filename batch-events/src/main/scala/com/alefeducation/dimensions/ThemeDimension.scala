package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Builder, Delta, DeltaSink}
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.DataFrameUtility.getUTCDateFrom
import com.alefeducation.util.{BatchTransformerUtility, SparkSessionUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, TimestampType}

import scala.collection.immutable.ListMap

class ThemeDimension(override implicit val session: SparkSession) extends SparkBatchService {

  import ThemeDimension._
  import session.implicits._

  override val name: String = JobName

  override def transform(): List[Sink] = {
    import BatchTransformerUtility._
    import Builder.Transformations
    import Delta.Transformations._

    val createdDF = readOptional(Parquet.CreatedConfig, session).map(_.cache())
    val updatedDF = readOptional(Parquet.UpdatedConfig, session).map(_.cache())
    val deletedDF = readOptional(Parquet.Deleted, session).map(_.cache())

    val outputCreatedDF = createdDF.map(
      _.transform(transformMutatedDF(_)("createdAt"))
        .withColumn("updatedAt", lit(null).cast(TimestampType))
        .transformForInsertDim(ThemeMutatedColumnMapping, EntityName, ids = List("id"))
        .cache())
    val outputUpdatedDF = updatedDF.map(
      _.transform(transformMutatedDF(_)("updatedAt"))
              .withColumn("createdAt", lit(null).cast(TimestampType))
              .transformForUpdateDim(ThemeMutatedColumnMapping, EntityName, List("id"))
        .cache())
    val outputDeletedDF = deletedDF.map(_.transformForDelete(ThemeDeletedColumnMapping, EntityName).cache())

    val deltaCreatedSink: Option[DeltaSink] =
      outputCreatedDF.flatMap(
        _.withEventDateColumn(false)
          .toCreate()
          .map(_.toSink(DeltaSink, eventType = ThemeCreatedEvent)))
    val deltaUpdatedSink: Option[DeltaSink] = outputUpdatedDF
      .flatMap(
        df =>
          df.withEventDateColumn(false)
            .toUpdate(updateColumns = df.columns.toList.diff(List("theme_created_at")))
            .map(_.toSink(DeltaSink, EntityName, ThemeUpdatedEvent)))
    val deltaDeletedSink: Option[DeltaSink] = outputDeletedDF
      .flatMap(
        _.withEventDateColumn(false)
          .toDelete()
          .map(_.toSink(DeltaSink, EntityName, ThemeDeletedEvent)))

    val redshiftCreatedSink =
      outputCreatedDF.map(
        _.transform(transformRedshiftMutatedDF)
          .toRedshiftInsertSink(RedshiftSink, ThemeCreatedEvent))
    val redshiftUpdatedSink =
      outputUpdatedDF.map(
        _.transform(transformRedshiftMutatedDF)
          .toRedshiftUpdateSink(RedshiftSink, ThemeUpdatedEvent, EntityName))
    val redshiftDeletedSink: Option[DataSink] = outputDeletedDF
      .map(_.toRedshiftUpdateSink(RedshiftSink, ThemeDeletedEvent, EntityName))

    val parquetCreatedSink = createdDF.map(_.toParquetSink(Parquet.CreatedConfig))
    val parquetUpdatedSink = updatedDF.map(_.toParquetSink(Parquet.UpdatedConfig))
    val parquetDeletedSink = deletedDF.map(_.toParquetSink(Parquet.Deleted))

    val deltaSinks = deltaCreatedSink ++ deltaUpdatedSink ++ deltaDeletedSink
    val redshiftSinks = redshiftCreatedSink ++ redshiftUpdatedSink ++ redshiftDeletedSink
    val parquetSinks = parquetCreatedSink ++ parquetUpdatedSink ++ parquetDeletedSink

    (deltaSinks ++ redshiftSinks ++ parquetSinks).toList
  }

  private def transformRedshiftMutatedDF(df: DataFrame) =
    df.drop(
      s"${EntityName}_thumbnail_file_name",
      s"${EntityName}_thumbnail_content_type",
      s"${EntityName}_thumbnail_file_size",
      s"${EntityName}_thumbnail_location",
      s"${EntityName}_thumbnail_updated_at",
      s"${EntityName}_description"
    )

  private def transformMutatedDF(df: DataFrame)(columnName: String) = {
    df.withColumn(s"$columnName", getUTCDateFrom($"$columnName").cast(TimestampType))
      .withColumn(s"thumbnailUpdatedAt", getUTCDateFrom($"thumbnailUpdatedAt").cast(TimestampType))
  }

}

object ThemeDimension {

  val JobName: String = "theme-dimension"
  val EntityName: String = "theme"

  val ThemeCreatedEvent: String = "ThemeCreatedEvent"
  val ThemeUpdatedEvent: String = "ThemeUpdatedEvent"
  val ThemeDeletedEvent: String = "ThemeDeletedEvent"

  val ThemeMutatedColumnMapping: Map[String, String] = ListMap(
    "id" -> s"${EntityName}_id",
    "name" -> s"${EntityName}_name",
    "description" -> s"${EntityName}_description",
    "boardId" -> s"${EntityName}_curriculum_id",
    "gradeId" -> s"${EntityName}_curriculum_grade_id",
    "subjectId" -> s"${EntityName}_curriculum_subject_id",
    "thumbnailFileName" -> s"${EntityName}_thumbnail_file_name",
    "thumbnailContentType" -> s"${EntityName}_thumbnail_content_type",
    "thumbnailFileSize" -> s"${EntityName}_thumbnail_file_size",
    "thumbnailLocation" -> s"${EntityName}_thumbnail_location",
    "thumbnailUpdatedAt" -> s"${EntityName}_thumbnail_updated_at",
    "createdAt" -> s"${EntityName}_created_at",
    "updatedAt" -> s"${EntityName}_updated_at",
    s"${EntityName}_status" -> s"${EntityName}_status",
    "occurredOn" -> "occurredOn"
  )

  val ThemeDeletedColumnMapping: Map[String, String] = ListMap(
    "id" -> s"${EntityName}_id",
    s"${EntityName}_status" -> s"${EntityName}_status",
    "occurredOn" -> "occurredOn"
  )

  val DeltaSink: String = "delta-theme-sink"
  val RedshiftSink: String = "redshift-theme-sink"

  object Parquet {
    val CreatedConfig: String = "parquet-theme-created"
    val UpdatedConfig: String = "parquet-theme-updated"
    val Deleted: String = "parquet-theme-deleted"
  }

  def apply(implicit session: SparkSession): ThemeDimension = new ThemeDimension

  def main(args: Array[String]): Unit = ThemeDimension(SparkSessionUtils.getSession(JobName)).run

}
