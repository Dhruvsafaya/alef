package com.alefeducation

import com.alefeducation.HeartbeatEvents.heartbeat
import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.Schema.heartbeatSchema
import com.alefeducation.service.{DataSink, SparkStreamingService}
import com.alefeducation.util.DataFrameUtility.process
import com.alefeducation.util.Resources.getPayloadStream
import com.alefeducation.util.{DateField, SparkSessionUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.LongType

class HeartbeatEvents(override val session: SparkSession) extends SparkStreamingService {
  override val name: String = HeartbeatEvents.name

  override def transform(): List[Sink] = {
    val kafkaStream = read("heartbeat-source", session)
    val payloadStream = getPayloadStream(session, kafkaStream)
    val dateOccurredOnTs = DateField("occurredOn", LongType)

    val sinks = List(
      DataSink(
        "heartbeat-sink",
        process(session, payloadStream, heartbeatSchema, List(heartbeat), dateOccurredOnTs)
      ),
    )
    sinks
  }
}

object HeartbeatEvents {

  val name = "heartbeat-events"
  final val heartbeat = "Heartbeat"

  def main(args: Array[String]): Unit = {
    new HeartbeatEvents(SparkSessionUtils.getSession(name)).run
  }
}
