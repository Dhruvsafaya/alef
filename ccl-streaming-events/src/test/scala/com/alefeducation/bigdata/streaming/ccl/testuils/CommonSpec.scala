package com.alefeducation.bigdata.streaming.ccl.testuils

import com.alefeducation.bigdata.Sink
import org.apache.spark.sql.types.StructType
import org.scalatest.matchers.should.Matchers

trait CommonSpec {

  def buildKafkaEvent(eventJson: String): String =
    s"""{"key": "key1","value":$eventJson,"timestamp": "2019-04-20 16:23:46.611"}"""

  def buildKafkaEvents(s: String): String = {
    val events = s.split("\n").toSeq.map(_.trim).filter(_ != "").map(buildKafkaEvent).mkString(",")
    s"[$events]"
  }

  def testSink(sinks: Seq[Sink], sinkName: String, schema: StructType, countOfEvents: Int = 1): Unit = {
    val sink = sinks.find(_.name == sinkName).getOrElse(throw new RuntimeException(s"$sinkName is not found"))
    val df = sink.output

    val dfSchema = df.schema.fields.map(f => f.name)
    val providedSchema = schema.fields.map(f => f.name) ++ Seq("eventType", "eventDateDw", "loadtime")

    new Matchers {
      dfSchema.diff(providedSchema) shouldBe empty
      df.count() shouldBe countOfEvents
    }
  }

}
