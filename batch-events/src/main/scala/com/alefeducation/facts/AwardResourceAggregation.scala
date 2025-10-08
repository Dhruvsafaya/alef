package com.alefeducation.facts

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Delta, DeltaSink}
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.Helpers.{StagingStarAwarded, _}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class AwardResourceAggregationTransformer(override val name: String, override implicit val session: SparkSession)
    extends SparkBatchService {
  override def transform(): List[Sink] = {

    import Delta.Transformations._
    import com.alefeducation.util.BatchTransformerUtility._

    val awardEventType = "AwardResource"
    val df: Option[DataFrame] = readWithExtraProps(ParquetAwardResourceSource, session, extraProps = List(("mergeSchema", "true")))
    val transDF: Option[DataFrame] = df.map(
      _.selectColumnsWithMapping(StagingStarAwarded)
        .appendTimestampColumns(FactStarAwardedEntity, isFact = true))

    val parquetSink: Option[DataSink] = df.map(_.toParquetSink(ParquetAwardResourceSource))
    val redshiftSink: Option[DataSink] = transDF.map(_.transform(transformCommentIsEmpty))
      .map(_.toRedshiftInsertSink(RedshiftStarAwardSink, awardEventType))

    val deltaSink: Option[DeltaSink] = transDF.flatMap { df =>
      val selCols = df.columns.map(
        k =>
          if (k.contains("uuid")) col(k).alias("fsa_" + k.replace("uuid", "id"))
          else col(k))
      df.select(selCols: _*)
        .withColumn("eventdate", date_format(col("fsa_created_time"), "yyyy-MM-dd"))
        .toCreate(isFact = true)
        .map(_.toSink(DeltaStarAwardSink, eventType = awardEventType))
    }

    (parquetSink ++ redshiftSink ++ deltaSink).toList
  }

  private def transformCommentIsEmpty(df: DataFrame): DataFrame =
    df.withColumn("fsa_award_comments", when(length(trim(col("fsa_award_comments"))) > 0, true).otherwise(false))

}

object AwardResourceAggregation {
  def main(args: Array[String]): Unit = {
    new AwardResourceAggregationTransformer(AwardResourceAggregationService, SparkSessionUtils.getSession(AwardResourceAggregationService)).run
  }
}
