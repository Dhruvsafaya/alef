package com.alefeducation.facts.learning_experience.delta

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.facts.learning_experience.delta.ExperienceTotalScoreUpdatedDelta.{FleTotalScoreState, LearningSessionDeltaSink, filterReplayed}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ExperienceTotalScoreUpdatedDelta(val session: SparkSession,
                                       val service: SparkBatchService) {
  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

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

    transformedTotalScoreUpdated.flatMap(
      _.toCreate(isFact = true)
        .map(_.toSink(LearningSessionDeltaSink, eventType = "totalScoreUpdatedEvent"))
    )
  }
}

object ExperienceTotalScoreUpdatedDelta {

  val LearningSessionDeltaService = "delta-learning-experience"

  val LearningSessionDeltaSink = "delta-learning-experience-sink"
  val FleTotalScoreState = 3

  val session = SparkSessionUtils.getSession(LearningSessionDeltaService)
  val service = new SparkBatchService(LearningSessionDeltaService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new ExperienceTotalScoreUpdatedDelta(session, service)
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

