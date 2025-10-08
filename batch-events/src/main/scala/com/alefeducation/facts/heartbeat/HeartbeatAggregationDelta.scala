package com.alefeducation.facts.heartbeat

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.batch.delta.Alias
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.TimestampType

object HeartbeatAggregationDelta {
  val serviceName = "delta-heartbeat-aggregated"
  val sourceName = "heartbeat-aggregated-transformed-sink"
  val sinkName = "delta-aggregated-heartbeat-sink"

  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  def main(args: Array[String]): Unit = {
    val columns: Seq[String] = Seq(
      "eventdate",
      "fuhha_created_time",
      "fuhha_dw_created_time",
      "fuhha_date_dw_id",
      "fuhha_user_id",
      "fuhha_role",
      "fuhha_school_id",
      "fuhha_content_repository_id",
      "fuhha_tenant_id",
      "fuhha_channel",
      "fuhha_activity_date_hour",
    )

    val df = service
      .readOptional(sourceName, session)
      .map(
        _.select(columns.head, columns.tail: _*)
          .withColumn("fuhha_dw_updated_time", lit(null).cast(TimestampType))
      )

    val matchConditions: String =
      s"""
        |${Alias.Delta}.fuhha_user_id = ${Alias.Events}.fuhha_user_id
        | AND ${Alias.Delta}.fuhha_activity_date_hour = ${Alias.Events}.fuhha_activity_date_hour
        |""".stripMargin

    val columnsToInsert: Map[String, String] = columns.map { column =>
      column -> s"${Alias.Events}.$column"
    }.toMap

    val columnsToUpdate
      : Map[String, String] = columnsToInsert - "fuhha_dw_created_time" + ("fuhha_dw_updated_time" -> s"${Alias.Events}.fuhha_dw_created_time")

    val deltaSink = df.flatMap(
      _.toUpsert(
        partitionBy = Seq("eventdate"),
        matchConditions = matchConditions,
        columnsToUpdate = columnsToUpdate,
        columnsToInsert = columnsToInsert
      ).map(_.toSink(sinkName))
    )
    service.run(deltaSink)
  }
}
