package com.alefeducation.facts.learning_experience_v2.transform.pathway

import com.alefeducation.facts.learning_experience.transform.LearningSessionAggregation.FleFinishedState
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

class PathwayLearningExperienceAggregation(override val serviceName: String) extends BaseTransform(serviceName) {
  import PathwayLearningExperienceAggregation._
  import spark.implicits._

  private val learningExperienceColsMapping: Map[String, String] = getNestedMap(serviceName, "column-mapping")

  override def transform(data: Map[String, Option[DataFrame]], startId: Long): Option[DataFrame] = {
    val learningSessionFinishedDf = data.get(LearningExperienceFinishedSource).flatten.map(filterReplayed)
    val learningSessionDeletedDf = data.get(LearningExperienceDiscardedSource).flatten.map(filterReplayed)

    val allExperiences = transformExperience(learningSessionFinishedDf)
      .unionOptionalByNameWithEmptyCheck(transformExperienceDiscarded(learningSessionDeletedDf), allowMissingColumns = true)

    val experiencesDF = allExperiences.map(
      _.withColumn("occurredOn", regexp_replace($"occurredOn", "T", " "))
        .withColumn("endTime", $"occurredOn".cast(DataTypes.TimestampType))
        .withColumn("startTime", regexp_replace($"startTime", "T", " ").cast(DataTypes.TimestampType))
        .withColumn("totalTime", unix_timestamp($"endTime", DateTimeFormat) - unix_timestamp($"startTime", DateTimeFormat))
        .withColumn("learningExperienceFlag", lit(true))
        .withColumn("retry", when($"attempt" > 1, true).otherwise(false))
        .withColumn("stars", when($"stars".isNotNull, $"stars").otherwise(DefaultStars))
        .withColumn("score", when($"score".isNotNull, $"score").otherwise(DefaultScore))
        .withColumn("totalScore", when($"totalScore".isNotNull, $"totalScore").otherwise(DefaultScore))
        .withColumn("timeSpent", when($"timeSpent".isNotNull, $"timeSpent").otherwise(lit(MinusOne)))
        .addColIfNotExists("activityComponentResources", schemaFor[List[ActivityComponent]].dataType.asInstanceOf[ArrayType])
        .withColumn(
          "activityComponentResources",
          when(
            $"activityComponentResources".isNotNull,
            from_json($"activityComponentResources".cast("string"), schemaFor[List[ActivityComponent]].dataType.asInstanceOf[ArrayType])
          ).otherwise(typedLit(Seq.empty[ActivityComponent]))
        )
        .addColIfNotExists("bonusStars", tp = IntegerType, value = "-1")
        .addColIfNotExists("bonusStarsScheme", tp = StringType, value = "NA")
        .withColumn("bonusStars", $"bonusStars".cast(DataTypes.IntegerType))
        .withColumn("totalStars", when($"totalStars".isNotNull, $"totalStars").otherwise(DefaultStars))
        .withColumn("activityCompleted", when($"activityCompleted".isNotNull, $"activityCompleted").otherwise(lit(false)))
        .withColumn("source", when($"source".isNotNull, $"source").otherwise(lit(null).cast(StringType)))
        .withColumn("teachingPeriodId", when($"teachingPeriodId".isNotNull, $"teachingPeriodId").otherwise(lit(null).cast(StringType)))
        .withColumn("academicYear", when($"academicYear".isNotNull, $"academicYear").otherwise(lit(null).cast(StringType)))
        .addColIfNotExists("activityTemplateId", StringType)
        .addColIfNotExists("openPathEnabled", BooleanType)
        .transform(addMaterialCols))

    experiencesDF.map(_.transformForInsertFact(learningExperienceColsMapping, "").genDwId(s"dw_id", startId))
  }

  private def transformExperience(experienceFinishedDf: Option[DataFrame]): Option[DataFrame] = {
    experienceFinishedDf.map(_.withColumn("state", lit(FleFinishedState)))
  }

  private def transformExperienceDiscarded(experienceFinishedDf: Option[DataFrame]): Option[DataFrame] = {
    experienceFinishedDf.map(_.withColumn("state", lit(FleDiscardedState)).withColumn("activityCompleted", lit(false)))
  }

  // todo: move this into a utility class outside
  private def filterReplayed(df: DataFrame): DataFrame = {
    if (!df.columns.contains("replayed")) return df

    df.filter($"replayed".isNull || !$"replayed").drop("replayed")
  }

}

object PathwayLearningExperienceAggregation {
  val LearningExperienceFinishedSource: String = Resources.getString("bronze-pathway-learning-experience-finished-source")
  val LearningExperienceDiscardedSource: String = Resources.getString("bronze-pathway-learning-experience-discarded-source")

  val FleDiscardedState = 4

  def main(args: Array[String]): Unit = {
    val serviceName = args(0)
    val transformer = new PathwayLearningExperienceAggregation(serviceName)
    transformer.run()
  }

}
