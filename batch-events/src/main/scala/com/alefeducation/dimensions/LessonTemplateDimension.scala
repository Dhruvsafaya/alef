package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Builder, Delta, DeltaSink}
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.{BatchTransformerUtility, SparkSessionUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.immutable.ListMap

class LessonTemplateDimension(override implicit val session: SparkSession) extends SparkBatchService {

  import LessonTemplateDimension._
  import session.implicits._

  override val name: String = JobName
  private val uniqueIdColumns = List("template_id")

  override def transform(): List[Sink] = {
    import BatchTransformerUtility._
    import Builder.Transformations
    import Delta.Transformations._

    val createdDF = readOptional(Parquet.CreatedConfig, session).map(_.cache())
    val updatedDF = readOptional(Parquet.UpdatedConfig, session).map(_.cache())

    val outputCreatedDF = createdDF.map(
      _.transform(transformMutatedDF)
        .transformForInsertDim(LessonTemplateMutatedColumnMapping, EntityName, ids = List("id", "stepId"))
        .cache())
    val outputIWHDF = updatedDF.map(
      _.transform(transformMutatedDF)
        .transformForInsertWithHistory(LessonTemplateMutatedColumnMapping, EntityName)
        .cache())

    val deltaCreatedSink: Option[DeltaSink] =
      outputCreatedDF.flatMap(
        _.withEventDateColumn(false)
          .toCreate()
          .map(_.toSink(DeltaSink, eventType = LessonTemplateCreatedEvent)))
    val deltaIWHSink: Option[DeltaSink] = outputIWHDF
      .flatMap(
        df =>
          df.withEventDateColumn(false)
            .toIWH(uniqueIdColumns = uniqueIdColumns, inactiveStatus = 2)
            .map(_.toSink(DeltaSink, EntityName)))

    val redshiftCreatedSink =
      outputCreatedDF.map(
        _.transform(transformRedshiftMutatedDF)
          .toRedshiftInsertSink(RedshiftSink, LessonTemplateCreatedEvent))
    val redshiftIWHSink =
      outputIWHDF.map(
        _.transform(transformRedshiftMutatedDF)
          .toRedshiftIWHSink(RedshiftSink,
                             EntityName,
                             eventType = LessonTemplateUpdatedEvent,
                             ids = List(s"${EntityName}_id"),
                             inactiveStatus = 2))

    val parquetCreatedSink = createdDF.map(_.toParquetSink(Parquet.CreatedConfig))
    val parquetUpdatedSink = updatedDF.map(_.toParquetSink(Parquet.UpdatedConfig))

    val deltaSinks = deltaCreatedSink ++ deltaIWHSink
    val redshiftSinks = redshiftCreatedSink ++ redshiftIWHSink
    val parquetSinks = parquetCreatedSink ++ parquetUpdatedSink

    (deltaSinks ++ redshiftSinks ++ parquetSinks).toList
  }

  private def transformRedshiftMutatedDF(df: DataFrame) =
    df.drop(s"${EntityName}_description", s"${EntityName}_step_code", s"${EntityName}_step_abbreviation")

  private def transformMutatedDF(df: DataFrame) = {
    val explodedDf = df
      .select($"*", explode($"steps").as("step"))
      .select($"*",
              $"step.id".as("stepId"),
              $"step.displayName".as("stepDisplayName"),
              $"step.stepId".as("stepCode"),
              $"step.abbreviation".as("stepAbbreviation"))
      .drop(s"step")

    (addStatusCol(EntityName, "occurredOn", List("id", "stepId")) _ andThen
      addUpdatedTimeCols(EntityName, "occurredOn", List("id", "stepId")))(explodedDf)
  }

}

object LessonTemplateDimension {

  val JobName: String = "lesson-template-dimension"
  val EntityName: String = "template"

  val LessonTemplateCreatedEvent: String = "LessonTemplateCreatedEvent"
  val LessonTemplateUpdatedEvent: String = "LessonTemplateUpdatedEvent"
  val LessonTemplateDeletedEvent: String = "LessonTemplateDeletedEvent"

  val LessonTemplateMutatedColumnMapping: Map[String, String] = ListMap(
    "id" -> s"${EntityName}_id",
    "frameworkId" -> s"${EntityName}_framework_id",
    "title" -> s"${EntityName}_title",
    "description" -> s"${EntityName}_description",
    "stepId" -> s"${EntityName}_step_id",
    "stepDisplayName" -> s"${EntityName}_step_display_name",
    "stepCode" -> s"${EntityName}_step_code",
    "stepAbbreviation" -> s"${EntityName}_step_abbreviation",
    s"${EntityName}_status" -> s"${EntityName}_status",
    "occurredOn" -> "occurredOn",
    "organisation" -> s"${EntityName}_organisation"
  )

  val LessonTemplateDeletedColumnMapping: Map[String, String] = ListMap(
    "id" -> s"${EntityName}_id",
    s"${EntityName}_status" -> s"${EntityName}_status",
    "occurredOn" -> "occurredOn"
  )

  val DeltaSink: String = "delta-lesson-template-sink"
  val RedshiftSink: String = "redshift-lesson-template-sink"

  object Parquet {
    val CreatedConfig: String = "parquet-lesson-template-created"
    val UpdatedConfig: String = "parquet-lesson-template-updated"
  }

  def apply(implicit session: SparkSession): LessonTemplateDimension = new LessonTemplateDimension

  def main(args: Array[String]): Unit = LessonTemplateDimension(SparkSessionUtils.getSession(JobName)).run

}
