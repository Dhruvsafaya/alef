package com.alefeducation.facts.heartbeat

import com.alefeducation.base.SparkBatchService
import com.alefeducation.io.data.RedshiftIO.TempTableAlias
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import com.alefeducation.util.BatchTransformerUtility._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType

object HeartbeatAggregationRedshift {
  val serviceName = "redshift-heartbeat-aggregated"
  val sourceName = "heartbeat-aggregated-transformed-sink"
  val sinkName = "redshift-aggregated-heartbeat-sink"
  val targetTableName = "staging_user_heartbeat_hourly_aggregated"

  val session: SparkSession = SparkSessionUtils.getSession(serviceName)
  val service = new SparkBatchService(serviceName, session)

  def main(args: Array[String]): Unit = {
    val df = service
      .readOptional(sourceName, session)
      .map(
        _.withColumn("fuhha_date_dw_id", col("fuhha_date_dw_id").cast(LongType))
      )

    val matchConditions: String =
      s"""
         |$targetTableName.fuhha_user_id = $TempTableAlias.fuhha_user_id
         | AND $targetTableName.fuhha_activity_date_hour = $TempTableAlias.fuhha_activity_date_hour
         |""".stripMargin

    val columns: Seq[String] = Seq(
      "fuhha_created_time",
      "fuhha_dw_created_time",
      "fuhha_date_dw_id",
      "fuhha_role",
      "fuhha_channel",
      "fuhha_activity_date_hour",
      "fuhha_tenant_id",
      "fuhha_user_id",
      "fuhha_school_id"
    )

    val columnsToInsert: Map[String, String] = columns.map { column =>
      column -> s"$TempTableAlias.$column"
    }.toMap

    //Update same columns except fuhha_dw_created_time. To keep track of updates, update fuhha_dw_updated_time instead
    val columnsToUpdate: Map[String, String] = columnsToInsert -
      "fuhha_dw_created_time" +
      ("fuhha_dw_updated_time" -> s"$TempTableAlias.fuhha_dw_created_time")

    val sink = df.map(
      _.toRedshiftUpsertSink(
        sinkName,
        targetTableName,
        matchConditions,
        columnsToUpdate,
        columnsToInsert,
        isStaging = true
      )
    )
    service.run(sink)
  }
}
