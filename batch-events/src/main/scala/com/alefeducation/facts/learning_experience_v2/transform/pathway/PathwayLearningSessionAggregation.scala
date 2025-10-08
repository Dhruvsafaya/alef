package com.alefeducation.facts.learning_experience_v2.transform.pathway

import com.alefeducation.schema.lps.ActivityComponent
import com.alefeducation.transformer.BaseTransform
import com.alefeducation.util.BatchTransformerUtility.{OptionalDataFrameTransformer, RawDataFrameToTransformationAndSink, addMaterialCols}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Resources
import com.alefeducation.util.Resources.getNestedMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class PathwayLearningSessionAggregation(serviceName: String) extends BaseTransform(serviceName: String) {
  import PathwayLearningSessionAggregation._
  import spark.implicits._

  private val learningSessionColsMapping: Map[String, String] = getNestedMap(serviceName, "column-mapping")

  override def transform(data: Map[String, Option[DataFrame]], startId: Long): Option[DataFrame] = {

    val learningSessionFinishedDf = data.get(LearningSessionFinishedSource).flatten.map(filterReplayed)
    val learningSessionDeletedDf = data.get(LearningSessionDeletedSource).flatten.map(filterReplayed)

    val learningSessionFinishedAndDeleted = transformSessionFinished(learningSessionFinishedDf)
      .unionOptionalByNameWithEmptyCheck(transformLearningSessionDeleted(learningSessionDeletedDf), allowMissingColumns = true)

    val learningSessions = learningSessionFinishedAndDeleted.map(
      df =>
        df.withColumn("experienceId", lit(DefaultStringValue))
          .withColumn("occurredOn", regexp_replace($"occurredOn", "T", " "))
          .withColumn("startTime", regexp_replace($"startTime", "T", " ").cast(DataTypes.TimestampType))
          .withColumn("endTime", $"occurredOn".cast(DataTypes.TimestampType))
          .withColumn("contentId", lit(DefaultStringValue))
          .withColumn("totalTime", unix_timestamp($"endTime", DateTimeFormat) - unix_timestamp($"startTime", DateTimeFormat))
          .withColumn("lessonType", lit(DefaultStringValue))
          .withColumn("retry", when($"attempt" > 1, true).otherwise(false))
          .withColumn("learningExperienceFlag", lit(false))
          .withColumn("stars", when($"stars".isNotNull, $"stars").otherwise(DefaultStars))
          .withColumn("score", when($"score".isNotNull, $"score").otherwise(DefaultScore))
          .withColumn("totalStars", when($"stars".isNotNull, $"stars").otherwise(DefaultStars))
          .withColumn("timeSpent", when($"timeSpent".isNotNull, $"timeSpent").otherwise(lit(MinusOne)))
          .withColumn("totalScore", when($"totalScore".isNotNull, $"totalScore").otherwise(lit(DefaultScore).cast(DoubleType)))
          .withColumn("redo", when($"redo".isNotNull, $"redo").otherwise(lit(false)))
          .withColumn("source", when($"source".isNotNull, $"source").otherwise(lit(null).cast(StringType)))
          .withColumn("activityCompleted", lit(true))
          .withColumn(
            "activityComponentResources",
            when(
              $"activityComponentResources".isNotNull,
              from_json($"activityComponentResources".cast("string"), schemaFor[List[ActivityComponent]].dataType.asInstanceOf[ArrayType])
            ).otherwise(typedLit(Seq.empty[ActivityComponent]))
          )
          .transform(addMaterialCols)
          .addColIfNotExists("bonusStars", tp = IntegerType, value = "-1")
          .addColIfNotExists("bonusStarsScheme", tp = StringType, value = "NA")
          .addColIfNotExists("openPathEnabled", BooleanType)
          .withColumn("bonusStars", $"bonusStars".cast(DataTypes.IntegerType))
          .enrichLearningExperienceColumns()
    )

    learningSessions.map(_.transformForInsertFact(learningSessionColsMapping, entity = "").genDwId(s"dw_id", startId))
  }

  def transformLearningSessionDeleted(learningSessionDeletedDf: Option[DataFrame]): Option[DataFrame] =
    learningSessionDeletedDf.map(_.withColumn("state", lit(FleDeletedState)))

  def transformSessionFinished(learningSessionFinishedDf: Option[DataFrame]): Option[DataFrame] =
    learningSessionFinishedDf.map(_.withColumn("state", lit(FleFinishedState)))

  private def filterReplayed(df: DataFrame): DataFrame = {
    if (!df.columns.contains("replayed")) return df

    df.filter($"replayed".isNull || !$"replayed").drop("replayed")
  }

}

object PathwayLearningSessionAggregation {
  val FleFinishedState = 2
  val FleDeletedState = 4
  val LearningSessionFinishedSource: String = Resources.getString("bronze-pathway-learning-session-finished-source")
  val LearningSessionDeletedSource: String = Resources.getString("bronze-pathway-learning-session-deleted-source")

  def main(args: Array[String]): Unit = {
    val serviceName = args(0)
    val transformer = new PathwayLearningSessionAggregation(serviceName)
    transformer.run()
  }
}
