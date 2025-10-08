package com.alefeducation.facts.learning_experience.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.schema.lps.ActivityComponent
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.alefeducation.facts.learning_experience.transform.LearningExperienceAggregation.filterReplayed

class LearningSessionAggregation(val session: SparkSession, val service: SparkBatchService){

  import LearningSessionAggregation._
  import session.implicits._

  def transform(): Option[DataSink] = {
    val learningSessionFinishedDf = service.readOptional(LearningSessionFinishedSource, session, extraProps = List(("mergeSchema", "true"))).map(
      _.transform(filterReplayed)
    )
    val learningSessionDeletedDf = service.readOptional(LearningSessionDeletedSource, session, extraProps = List(("mergeSchema", "true"))).map(
      _.transform(filterReplayed)
    )

    val learningSessionFinishedAndDeleted = transformSessionFinished(learningSessionFinishedDf).unionOptionalByNameWithEmptyCheck(transformedLearningSessionDeleted(learningSessionDeletedDf), allowMissingColumns = true)
    val learningSessions = learningSessionFinishedAndDeleted.map(df =>
      df.withColumn("experienceId", lit(DefaultStringValue))
        .withColumn("contentId", lit(DefaultStringValue))
        .withColumn("totalTime", unix_timestamp($"endTime", DateTimeFormat) - unix_timestamp($"startTime", DateTimeFormat))
        .withColumn("lessonType", lit(DefaultStringValue))
        .withColumn("retry", when($"attempt" > 1, true).otherwise(false))
        .withColumn("learningExperienceFlag", lit(false))
        .withColumn("stars", when($"stars".isNotNull, $"stars").otherwise(DefaultStars))
        .withColumn("score", when($"score".isNotNull, $"score").otherwise(DefaultScore))
        .withColumn("totalStars", when($"stars".isNotNull, $"stars").otherwise(DefaultStars))
        .withColumn("activityCompleted", lit(true))
        .withColumn("activityComponentResources", when($"activityComponentResources".isNotNull, $"activityComponentResources")
          .otherwise(typedLit(Seq.empty[ActivityComponent])))
        .transform(addMaterialCols)
        .addColIfNotExists("openPathEnabled", BooleanType)
        .enrichLearningExperienceColumns()
    )

    val learningSessionsTransformed = learningSessions.map(_.transformForInsertFact(LearningSessionCols,"fle"))

    learningSessionsTransformed.map(DataSink(TransformedLearningSessionSink, _))
  }


  private def transformedLearningSessionDeleted(learningSessionDeletedDf: Option[DataFrame]): Option[DataFrame] = {
    learningSessionDeletedDf.map(_.withColumn("occurredOn", regexp_replace($"occurredOn", "T", " "))
      .withColumn("startTime", regexp_replace($"startTime", "T", " "))
      .withColumn("endTime", $"occurredOn")
      .withColumn("state", lit(FleDeletedState))
      .withColumn("redo", lit(false))
      .withColumn("stars", lit(DefaultStars))
      .withColumn("score", lit(DefaultScore).cast(DoubleType))
      .withColumn("totalScore", lit(DefaultScore).cast(DoubleType))
      .withColumn("timeSpent", lit(MinusOne))
      .withColumn("activityComponentResources", typedLit(Seq.empty[ActivityComponent]))
      .withColumn("source", lit(null).cast(StringType))
      .addColIfNotExists("bonusStars",tp = IntegerType, value="-1")
      .addColIfNotExists("bonusStarsScheme",tp = StringType, value="NA")
    )
  }

  private def transformSessionFinished(learningSessionFinishedDf: Option[DataFrame]): Option[DataFrame] = {
    import com.alefeducation.util.BatchTransformerUtility._
    learningSessionFinishedDf.map(_
      .withColumn("occurredOn", regexp_replace($"occurredOn", "T", " "))
      .withColumn("startTime", regexp_replace($"startTime", "T", " "))
      .withColumn("endTime", $"occurredOn")
      .withColumn("state", lit(FleFinishedState))
      .withColumn("activityComponentResources", from_json($"activityComponentResources".cast("string"), schemaFor[List[ActivityComponent]].dataType.asInstanceOf[ArrayType]))
      .addColIfNotExists("bonusStars",tp = IntegerType, value="-1")
      .addColIfNotExists("bonusStarsScheme",tp = StringType, value="NA")
    )
  }

}

object LearningSessionAggregation {

  val FleFinishedState = 2
  val FleDeletedState = 4
  val FactLearningExperienceEntity = "fle"

  val LearningSessionTransformService = "transform-learning-aggregation"

  val LearningSessionFinishedSource = "parquet-learning-session-finished-source"
  val LearningSessionDeletedSource = "parquet-learning-session-deleted-source"

  val TransformedLearningSessionSink = "transformed-learning-session-sink"

  val session = SparkSessionUtils.getSession(LearningSessionTransformService)
  val service = new SparkBatchService(LearningSessionTransformService, session)

  def main(args: Array[String]): Unit = {
    val transform = new LearningSessionAggregation(session, service)
    service.run(transform.transform())
  }

}
