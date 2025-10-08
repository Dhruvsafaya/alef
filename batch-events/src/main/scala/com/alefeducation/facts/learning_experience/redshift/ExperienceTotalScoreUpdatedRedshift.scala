package com.alefeducation.facts.learning_experience.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.facts.learning_experience.redshift.ExperienceTotalScoreUpdatedRedshift.{FleTotalScoreState, LearningSessionRedshiftSink, filterReplayed}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ExperienceTotalScoreUpdatedRedshift(val session: SparkSession,
                                          val service: SparkBatchService) {

  def transform(): Option[Sink] = {
    import com.alefeducation.util.BatchTransformerUtility._

    val totalScoreUpdated = service.readOptional(ParquetTotalScoreUpdatedSource, session)

    val transformedTotalScoreUpdated = totalScoreUpdated.map(
      _.transform(filterReplayed)
        .withColumn("occurredOn", regexp_replace(col("occurredOn"), "T", " "))
        .withColumn("startTime", regexp_replace(col("startTime"), "T", " "))
        .withColumn("endTime", col("occurredOn"))
        .withColumn("totalTime", unix_timestamp(col("endTime"), DateTimeFormat) - unix_timestamp(col("startTime"), DateTimeFormat))
        .withColumn("retry", col("attempt") > 1)
        .withColumn("lessonType", lit("Unlock"))
        .withColumn("state", lit(FleTotalScoreState))
        .withColumn("stars", when(col("stars").isNotNull, col("stars")).otherwise(DefaultStars))
        .withColumn("totalStars", col("stars"))
        .addColIfNotExists("openPathEnabled", BooleanType)
        .addColIfNotExists("bonusStars", tp = IntegerType, value = "-1")
        .addColIfNotExists("bonusStarsScheme", tp = StringType, value = "NA")
        .enrichLearningExperienceColumns()
        .transformForInsertFact(ExperienceTotalScoreUpdatedCols, FactLearningExperienceEntity, List("uuid"))
    )

      transformedTotalScoreUpdated.map(
        _.drop("eventdate", "fle_activity_components")
          .toRedshiftInsertSink(LearningSessionRedshiftSink, "totalScoreUpdatedEvent")
      )
  }
}

object ExperienceTotalScoreUpdatedRedshift {

  val LearningSessionRedshiftService = "redshift-learning-experience"

  val LearningSessionRedshiftSink = "redshift-learning-experience-sink"
  val FleTotalScoreState = 3

  val session = SparkSessionUtils.getSession(LearningSessionRedshiftService)
  val service = new SparkBatchService(LearningSessionRedshiftService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new ExperienceTotalScoreUpdatedRedshift(session, service)
    service.run(transformer.transform())
  }

  def filterReplayed(df: DataFrame): DataFrame = {
    import session.implicits._

    if (!df.columns.contains("replayed")) {
      return df
    }

    df.filter($"replayed".isNull || !$"replayed").drop("replayed")
  }
}

