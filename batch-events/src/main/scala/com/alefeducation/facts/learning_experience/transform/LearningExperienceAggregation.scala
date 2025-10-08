package com.alefeducation.facts.learning_experience.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.learning_experience.transform.LearningSessionAggregation.FleFinishedState
import com.alefeducation.schema.lps.ActivityComponent
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers._
import com.alefeducation.util.{Logging, SparkSessionUtils}
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, BooleanType, IntegerType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class LearningExperienceAggregation(val session: SparkSession, val service: SparkBatchService) extends Logging{

  import LearningExperienceAggregation._
  import session.implicits._

  def transform(): Option[DataSink] = {
    import com.alefeducation.util.BatchTransformerUtility._

    val experienceFinishedDf = service.readOptional(LearningExperienceFinishedSource, session, extraProps = List(("mergeSchema", "true"))).map(
      _.transform(filterReplayed)
    )
    val experienceDiscardedDf = service.readOptional(LearningExperienceDiscardedSource, session, extraProps = List(("mergeSchema", "true"))).map(
      _.transform(filterReplayed)
    )

    //Transformations
    val experienceFinished = transformExperience(experienceFinishedDf)
    val experienceDiscarded = transformExperienceDiscarded(experienceDiscardedDf)

    val allExperiences = experienceFinished.unionOptionalByNameWithEmptyCheck(experienceDiscarded, allowMissingColumns = true)

    val experiencesDF = allExperiences.map(
      _.withColumn("startTime", $"startTimeFromFinish")
        .withColumn("totalTime", unix_timestamp($"endTime", DateTimeFormat) - unix_timestamp($"startTime", DateTimeFormat))
        .withColumn("learningExperienceFlag", lit(true))
        .withColumn("retry", when($"attempt" > 1, true).otherwise(false))
        .withColumn("stars", when($"stars".isNotNull, $"stars").otherwise(DefaultStars))
        .withColumn("isInClassGameExperience", when($"isInClassGameExperience".isNotNull, $"isInClassGameExperience").otherwise(false))
        .withColumn("score", when($"score".isNotNull, $"score").otherwise(DefaultScore))
        .withColumn("totalScore", when($"totalScore".isNotNull, $"totalScore").otherwise(DefaultScore))
        .withColumn("activityComponentResources", when($"activityComponentResources".isNotNull, $"activityComponentResources")
          .otherwise(typedLit(Seq.empty[ActivityComponent])))
        .addColIfNotExists("activityTemplateId",StringType)
        .addColIfNotExists("openPathEnabled", BooleanType)
        .transform(addMaterialCols))

    val learningExperienceTransformed = experiencesDF.map(_.transformForInsertFact(LearningExperienceCols, "fle"))

    learningExperienceTransformed.map(DataSink(TransformedLearningExperienceSink, _))
}

  private def transformExperience(experienceFinishedDf: Option[DataFrame]): Option[DataFrame] = {
    transformExp(experienceFinishedDf).map(_
      .withColumn("totalStars", when($"totalStars".isNotNull, $"totalStars").otherwise(DefaultStars))
      .withColumn("state", lit(FleFinishedState))
    )
  }

  private def transformExperienceDiscarded(experienceFinishedDf: Option[DataFrame]): Option[DataFrame] = {
    transformExp(experienceFinishedDf).map(
      _.withColumn("state", lit(FleDiscardedState))
        .withColumn("timeSpent", lit(MinusOne))
        .withColumn("totalStars", lit(DefaultStars))
        .withColumn("activityCompleted", lit(false))
        .withColumn("source", lit(null).cast(StringType))
        .withColumn("teachingPeriodId", lit(null).cast(StringType))
        .withColumn("academicYear", lit(null).cast(StringType))
    )
  }

  private def transformExp(df: Option[DataFrame]): Option[DataFrame] = {
    import com.alefeducation.util.BatchTransformerUtility._
    df.map(
      _.withColumn("occurredOn", regexp_replace($"occurredOn", "T", " "))
        .withColumn("startTime", regexp_replace($"startTime", "T", " "))
        .withColumn("endTime", $"occurredOn")
        .withColumnRenamed("startTime", "startTimeFromFinish")
        .withColumn("activityComponentResources", from_json($"activityComponentResources".cast("string"),
          schemaFor[List[ActivityComponent]].dataType.asInstanceOf[ArrayType]))
        .addColIfNotExists("bonusStars",tp = IntegerType, value="-1")
        .addColIfNotExists("bonusStarsScheme",tp = StringType, value="NA")
    )
  }
}

object LearningExperienceAggregation {

  val FleDiscardedState = 4

  val DeltaSinkName = "delta-learning-experience-sink"

  val LearningExperienceTransformService = "transform-experience-aggregation"

  val LearningExperienceFinishedSource = "parquet-experience-finished-source"
  val LearningExperienceDiscardedSource = "parquet-experience-discarded-source"

  val TransformedLearningExperienceSink = "transformed-learning-experience-sink"

  val session = SparkSessionUtils.getSession(LearningExperienceTransformService)
  val service = new SparkBatchService(LearningExperienceTransformService, session)

  def filterReplayed(df: DataFrame): DataFrame = {
    import session.implicits._

    if (!df.columns.contains("replayed")) {
      return df
    }

    df.filter($"replayed".isNull || !$"replayed").drop("replayed")
  }

  def main(args: Array[String]): Unit = {
    val transform = new LearningExperienceAggregation(session, service)
    service.run(transform.transform())
  }

}