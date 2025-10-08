package com.alefeducation.facts

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Delta.Transformations._
import com.alefeducation.facts.learning_experience.transform.LearningExperienceAggregation.filterReplayed
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.Constants.ExperienceSubmitted
import com.alefeducation.util.Helpers._
import com.alefeducation.util.{BatchTransformerUtility, SparkSessionUtils}
import org.apache.spark.sql.functions.{col, lit, regexp_replace, when}
import org.apache.spark.sql.types.{BooleanType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ExperienceSubmittedBatch(val name: String, val session: SparkSession) extends SparkBatchService {

  import BatchTransformerUtility._

  private val uniqueId = "uuid"

  private val prefix = s"${FactExperienceSubmittedEntity}_"

  override def transform(): List[Sink] = {
    val experienceSubmittedDf = readOptional(ParquetExperienceSubmittedSource, session)

    val parquetExperienceSubmittedSink = experienceSubmittedDf.map(_.toParquetSink(ParquetExperienceSubmittedSink))

    val experienceSubmittedTransformed = experienceSubmittedDf.map(
      _.transform(filterReplayed)
        .transform(addMaterialCols)
        .addColIfNotExists("openPathEnabled", BooleanType)
       .transformForInsertFact(
        ExperienceSubmittedCols,
        FactExperienceSubmittedEntity,
        List(uniqueId)
      ).transform(transformStartTime)
    )

    val redshiftExperienceSubmittedSink = experienceSubmittedTransformed.map(
      _.toRedshiftInsertSink(RedshiftExperienceSubmittedSink, ExperienceSubmitted)
    )

    val deltaExperienceSubmittedSink = experienceSubmittedTransformed.flatMap(
      _.renameUUIDcolsForDelta(Some(prefix))
        .toCreate(isFact = true).map(_.toSink(DeltaExperienceSubmittedSink))
    )

    (parquetExperienceSubmittedSink ++ redshiftExperienceSubmittedSink ++ deltaExperienceSubmittedSink).toList
  }

  def transformStartTime(df: DataFrame): DataFrame = {
    df.withColumn(s"${prefix}start_time", regexp_replace(col(s"${prefix}start_time"), "T", " ")
      .cast(TimestampType))
  }
}

object ExperienceSubmittedBatch {
  def main(args: Array[String]): Unit = {
    new ExperienceSubmittedBatch(ExperienceSubmittedName, SparkSessionUtils.getSession(ExperienceSubmittedName)).run
  }
}