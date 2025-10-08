package com.alefeducation

import com.alefeducation.schema.Schema._
import com.alefeducation.service.{DataSink, SparkStreamingService}
import com.alefeducation.util.DataFrameUtility.process
import com.alefeducation.util.Resources.getPayloadStream
import com.alefeducation.util.{DateField, SparkSessionUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType

class NewXAPIRawEventsTransformer(override val name: String, override val session: SparkSession) extends SparkStreamingService {

  override def transform(): List[DataSink] = {
    val kafkaStream = read("new-xapi-source", session)
    val dateField = DateField("timestamp", TimestampType)
    val payload = getPayloadStream(session, kafkaStream)
    import session.implicits._
    val filtered = payload.filter($"eventType".rlike("^[\\w\\.]+$"))

    val transformedDF = process(session, filtered, newXAPISchema, List.empty, dateField)
      .withColumn("eventdate", date_format($"occurredOn", "yyyy-MM-dd"))
    val rawXapiDF = filtered.withColumn("eventdate", date_format($"loadtime", "yyyy-MM-dd"))

     val guardianRawEvents =  rawXapiDF.filter($"eventType" like "guardian.app%")
     val studentRawEvents =  rawXapiDF.filter($"eventType" like "student%")
     val teacherRawEvents =  rawXapiDF.filter($"eventType" like "teacher%")

    List(
      DataSink("new-xapi-sink", transformedDF, Map("partitionBy" -> "eventdate,eventType")),
      DataSink("raw-guardian-xapi-sink", guardianRawEvents, Map("partitionBy" -> "eventdate,eventType")),
      DataSink("raw-student-xapi-sink", studentRawEvents, Map("partitionBy" -> "eventdate,eventType")),
      DataSink("raw-teacher-xapi-sink", teacherRawEvents, Map("partitionBy" -> "eventdate,eventType"))
    )
  }
}

object NewXAPIRawEvents {
  def main(args: Array[String]): Unit = {
    val name = "new-xapi-events"
    new NewXAPIRawEventsTransformer(name, SparkSessionUtils.getSession(name)).run
  }
}