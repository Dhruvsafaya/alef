package com.alefeducation.facts.heartbeat

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.heartbeat.HeartbeatTransform._
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{SparkSession}

import scala.collection.immutable.Map

class HeartbeatTransform(val session: SparkSession,
                         val service: SparkBatchService) {

  def transform(): Option[DataSink] = {
    val heartbeatParquet = service.readOptional(ParquetHeartbeatSource, session)

    val heartbeatTransformed = heartbeatParquet.map(
      _.transformForInsertFact(HeartbeatCols, FactHeartbeatEntity)
    )

    heartbeatTransformed.map(df => {
      DataSink(TransformHeartbeatSink, df)
    })
  }

}

object HeartbeatTransform {
  val HeartbeatTransformService = "transform-heartbeat"
  val ParquetHeartbeatSource = "parquet-heartbeat-source"
  val FactHeartbeatEntity = "fuhe"
  val TransformHeartbeatSink = "heartbeat-transformed-sink"

  val HeartbeatCols = Map[String, String](
    "uuid" -> "fuhe_user_id",
    "role" -> "fuhe_role",
    "channel" -> "fuhe_channel",
    "occurredOn" -> "occurredOn",
    "tenantId" -> "fuhe_tenant_id",
    "schoolId"  -> "fuhe_school_id"
  )

  val session: SparkSession = SparkSessionUtils.getSession(HeartbeatTransformService)
  val service = new SparkBatchService(HeartbeatTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new HeartbeatTransform(session, service)
    service.run(transformer.transform())
  }
  
}