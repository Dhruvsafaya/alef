package com.alefeducation.facts.heartbeat

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.heartbeat.HeartbeatAggregationTransform._
import com.alefeducation.models.StudentModel.SchoolDim
import com.alefeducation.models.UserModel.User
import com.alefeducation.schema.admin.{ContentRepositoryRedshift, DwIdMapping}
import com.alefeducation.schema.internal.ControlTableUtils.DataOffsetType
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers.{RedshiftContentRepositorySource, RedshiftDwIdMappingSource, RedshiftSchoolSink, RedshiftUserSink}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.sql.Timestamp
import java.time.temporal.ChronoUnit

class HeartbeatAggregationTransform(val session: SparkSession, val service: SparkBatchService) {

  import session.implicits._

  def transform(): Option[DataSink] = {
    //Get latest processed timestamp
    val start: Timestamp = service.getDataOffset(HeartbeatOffsetTimestampKey)

    //Get raw data chunk which wasn't processed
    val rawDeltaChunk: Option[DataFrame] = getRawDataChunk(start)

    //Aggregate a new batch of data hourly
    val heartbeatTransformed: Option[DataFrame] = aggregateHeartbeat(rawDeltaChunk, ChronoUnit.HOURS)

    //Enrich with dims data
    val redshiftRelUser: DataFrame = service
      .readFromRedshift[User](
        redshiftSource = RedshiftUserSink,
        withoutDropDuplicates = true
      )
      .distinct()

    val redshiftSchool: DataFrame = service
      .readFromRedshift[SchoolDim](RedshiftSchoolSink)
      .select("school_id", "school_dw_id", "school_content_repository_dw_id")
      .cache()

    val redshiftContentRepository: DataFrame = service
      .readFromRedshift[ContentRepositoryRedshift](RedshiftContentRepositorySource)
      .cache()

    val enrichedUserHeartbeat: Option[DataFrame] = getUserEnrichedHeartbeat(
      heartbeatTransformed,
      redshiftRelUser,
      redshiftSchool,
      redshiftContentRepository
    )

    //Sink
    enrichedUserHeartbeat
      .map(df => {
        DataSink(TransformHeartbeatAggregatedSink,
                 df.cache(),
                 controlTableUpdateOptions = Map(DataOffsetType -> HeartbeatOffsetTimestampKey))
      })

  }

  def getRawDataChunk(lastTimestamp: Timestamp): Option[DataFrame] = {
    service
      .readOptional(DeltaHeartbeatRaw, session)
      .map(
        _.filter(col("fuhe_dw_created_time") > lastTimestamp)
      )
  }

  def aggregateHeartbeat(rawDelta: Option[DataFrame], aggregationBatchUnit: ChronoUnit): Option[DataFrame] = {
    val w = Window.partitionBy($"fuhha_user_id", $"fuhha_activity_date_hour").orderBy($"fuhha_created_time".desc)
    rawDelta.map(
      _.withColumn("eventType", lit("Heartbeat")) //required for transformForInsertFact
        .withColumnRenamed("fuhe_created_time", "occurredOn") //required for transformForInsertFact
        .transformForInsertFact(HeartbeatAggregatedCols, FactHeartbeatAggregatedEntity)
        .withColumn("fuhha_activity_date_hour", date_trunc(aggregationBatchUnit.name().dropRight(1), $"fuhha_created_time"))
        .withColumn("rank", row_number().over(w))
        .where($"rank" === 1)
        .drop("rank")
    )
  }

  def getUserEnrichedHeartbeat(heartbeat: Option[DataFrame],
                               redshiftRelUser: DataFrame,
                               redshiftSchool: DataFrame,
                               redshiftContentRepository: DataFrame): Option[DataFrame] = {

    heartbeat.map(
      _.join(
          redshiftRelUser,
          $"fuhha_user_id" === $"user_id",
          joinType = "left"
        )
        .join(redshiftSchool, $"fuhha_school_id" === $"school_id", joinType = "left")
        .join(redshiftContentRepository, $"school_content_repository_dw_id" === $"content_repository_dw_id", joinType = "left")
        .withColumnRenamed("user_dw_id", "fuhha_user_dw_id")
        .withColumnRenamed("school_dw_id", "fuhha_school_dw_id")
        .withColumnRenamed("school_content_repository_dw_id", "fuhha_content_repository_dw_id")
        .withColumnRenamed("content_repository_id", "fuhha_content_repository_id")
        .drop("content_repository_dw_id")
    )
  }


}

object HeartbeatAggregationTransform {
  val DeltaHeartbeatRaw = "delta-heartbeat-sink"
  val HeartbeatAggregatedTransformService = "transform-heartbeat-aggregated"
  val FactHeartbeatAggregatedEntity = "fuhha"
  val TransformHeartbeatAggregatedSink = "heartbeat-aggregated-transformed-sink"

  val HeartbeatOffsetTimestampKey = "fact_user_heartbeat_hourly_aggregated"

  val HeartbeatAggregatedCols: Map[String, String] = Map[String, String](
    "fuhe_user_id" -> "fuhha_user_id",
    "fuhe_role" -> "fuhha_role",
    "fuhe_channel" -> "fuhha_channel",
    "occurredOn" -> "occurredOn",
    "fuhe_tenant_id" -> "fuhha_tenant_id",
    "fuhe_school_id" -> "fuhha_school_id",
    "fuhe_dw_created_time" -> "fuhe_dw_created_time"
  )

  val session: SparkSession = SparkSessionUtils.getSession(HeartbeatAggregatedTransformService)
  val service = new SparkBatchService(HeartbeatAggregatedTransformService, session, Some("fuhe"))

  def main(args: Array[String]): Unit = {
    val transformer = new HeartbeatAggregationTransform(session, service)
    service.run(transformer.transform())
  }

}
