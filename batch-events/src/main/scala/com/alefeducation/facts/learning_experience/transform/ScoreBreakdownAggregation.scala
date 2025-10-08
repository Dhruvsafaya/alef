package com.alefeducation.facts.learning_experience.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.learning_experience.transform.LearningExperienceAggregation.filterReplayed
import com.alefeducation.schema.lps.ScoreBreakdownWithLessonIds
import com.alefeducation.service.DataSink
import com.alefeducation.util.{DateTimeProvider, FactTransformer, Logging, SparkSessionUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.alefeducation.facts.learning_experience.transform.ScoreBreakdownAggregation.{LearningExperienceFinishedSource, SCBSchema, TransformedScoreBreakdownSink}

class ScoreBreakdownAggregation(val session: SparkSession, val service: SparkBatchService, dateTimeProvider: DateTimeProvider = new DateTimeProvider) extends Logging{


  def transform(): Option[DataSink]  = {

    val emptySCBDF = session.createDataFrame(session.sparkContext.emptyRDD[Row], SCBSchema)

    val experienceFinishedDf = service.readOptional(LearningExperienceFinishedSource, session).map(
      _.transform(filterReplayed)
        .select("experienceId", "learningSessionId", "scoreBreakDown", "occurredOn")
    )

    if(!isScoreBreakdownMissing(experienceFinishedDf.getOrElse(emptySCBDF))){
      val transformedDF = transformScore(experienceFinishedDf.getOrElse(emptySCBDF))
      transformedDF.map(DataSink(TransformedScoreBreakdownSink, _))
    }
    else{
      Some(emptySCBDF).map(DataSink(TransformedScoreBreakdownSink, _))
    }
  }

  private def isScoreBreakdownMissing(df: DataFrame): Boolean = {
    df.columns.contains("scoreBreakDown") match {
      case false =>
        log.warn("Skipping ScoreBreakdown Sink. Found `scoreBreakDown` field missing.")
        true

      case true =>
        val scoreBreakdownField = df.schema.fields.find(_.name == "scoreBreakDown")
        scoreBreakdownField.get.dataType match {
          case sbType: ArrayType if sbType.elementType.isInstanceOf[StructType] =>
            log.info("`scoreBreakDown` field is found having proper data type.")
            df.filter(size(col("scoreBreakDown")) > 0).isEmpty

          case sbType: ArrayType =>
            log.warn(s"Skipping ScoreBreakdown Sink. Expected `scoreBreakDown` field to be an array of struct type but found array of ${sbType.elementType}.")
            true

          case otherType =>
            log.warn(s"Skipping ScoreBreakdown Sink. Expected `scoreBreakDown` field to be an array but found ${otherType.typeName}.")
            true
        }
    }
  }


  def transformScore(experienceFinished: DataFrame): Option[DataFrame] = {
    val spark = experienceFinished.sparkSession
    import spark.implicits._

    val scoreBreakdownDF = experienceFinished
      .withColumn("sb", explode($"scoreBreakDown"))
      .select($"occurredOn", $"experienceId", $"learningSessionId", $"sb.*")

    val scoreBreakdownWithAllCols = addMissingColumns(scoreBreakdownDF)

    val columnPrefix = "fle_scbd"
    val factTransformer = new FactTransformer(columnPrefix, dateTimeProvider)

    val scoreBreakdownRecords = scoreBreakdownWithAllCols
      .transform(factTransformer.appendGeneratedColumns)
      .select(
        $"fle_scbd_created_time",
        $"fle_scbd_dw_created_time",
        $"fle_scbd_date_dw_id".cast(IntegerType),
        $"experienceId".as("fle_scbd_fle_exp_id"),
        $"learningSessionId".as("fle_scbd_fle_ls_id"),
        $"id".as("fle_scbd_question_id"),
        $"code".as("fle_scbd_code"),
        $"timeSpent".cast(DoubleType).as("fle_scbd_time_spent"),
        $"hintsUsed".cast(BooleanType).as("fle_scbd_hints_used"),
        $"maxScore".cast(DoubleType).as("fle_scbd_max_score"),
        $"score".cast(DoubleType).as("fle_scbd_score"),
        explode_outer($"lessonIds").as("fle_scbd_lo_id"),
        $"type".as("fle_scbd_type"),
        $"version".cast(IntegerType).as("fle_scbd_version"),
        $"isAttended".cast(BooleanType).as("fle_scbd_is_attended"),
        $"eventdate"
      )
      .na.fill(Map("fle_scbd_lo_id" -> "n/a"))

    Some(scoreBreakdownRecords)
  }

  private def addMissingColumns(scoreBreakdownDF: DataFrame): DataFrame = {
    val spark = scoreBreakdownDF.sparkSession
    val scoreBreakdownExpectedSchema = spark.createDataFrame(List.empty[ScoreBreakdownWithLessonIds]).schema.fields

    val dfWithAllCols = scoreBreakdownExpectedSchema
      .foldLeft(scoreBreakdownDF) {
        case (df, field) =>
          if (!df.columns.contains(field.name))
            df.withColumn(field.name, lit(null).cast(field.dataType))
          else df
      }

    if (dfWithAllCols.schema.find(_.name == "lessonIds").get.dataType != ArrayType(StringType)) {
      log.info(s"ScoreBreakdown lessonIds field contains `null` value.")
      dfWithAllCols.withColumn("lessonIds", lit(array()).cast(ArrayType(StringType)))
    } else dfWithAllCols
  }
}
object ScoreBreakdownAggregation {

  val ScoreBreakdownTransformService = "transform-score-breakdown"

  val LearningExperienceFinishedSource = "parquet-experience-finished-source"

  val TransformedScoreBreakdownSink = "transformed-score-breakdown-sink"

  val session = SparkSessionUtils.getSession(ScoreBreakdownTransformService)
  val service = new SparkBatchService(ScoreBreakdownTransformService, session)

  val SCBSchema = StructType(
    StructField("fle_scbd_created_time", TimestampType, true) ::
      StructField("fle_scbd_dw_created_time", TimestampType, true) ::
      StructField("fle_scbd_date_dw_id", StringType, true) ::
      StructField("fle_scbd_fle_exp_id", StringType, true) ::
      StructField("fle_scbd_fle_ls_id", StringType, true) ::
      StructField("fle_scbd_question_id", LongType, true) ::
      StructField("fle_scbd_code", LongType, true) ::
      StructField("fle_scbd_time_spent", LongType, true) ::
      StructField("fle_scbd_hints_used", LongType, true) ::
      StructField("fle_scbd_max_score", DoubleType, true) ::
      StructField("fle_scbd_score", DoubleType, true) ::
      StructField("fle_scbd_lo_id", StringType, true) ::
      StructField("fle_scbd_type", StringType, true) ::
      StructField("fle_scbd_version", LongType, true) ::
      StructField("fle_scbd_is_attended", BooleanType, true) ::
      StructField("eventdate", LongType, true) :: Nil
  )

  def main(args: Array[String]): Unit = {
    val transform = new ScoreBreakdownAggregation(session, service)
    service.run(transform.transform())
  }

}
