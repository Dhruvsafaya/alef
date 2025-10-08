package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.scalatest.matchers.should.Matchers

class HeartbeatEventsSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer = new HeartbeatEvents(spark)
  }

  test("handle heartbeat events") {
    new Setup {
      val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "loadtime", "occurredOn","tenantId", "role", "channel", "uuid", "schoolId")
      val fixtures = List(
        SparkFixture(
          key = "heartbeat-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"Heartbeat",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |     "uuid": "45e4949d-7eff-4707-9201-dac917a5e013",
                    |     "role": "STUDENT",
                    |     "channel": "WEB",
                    |     "schoolId": "45e4949d-7eff-4707-9201-dac917a5e013",
                    |     "occurredOn": 100500092323
                    |   }
                    | },
                    | "timestamp": "2023-05-15 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "heartbeat-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "Heartbeat"
            fst.getAs[String]("role") shouldBe "STUDENT"
            fst.getAs[String]("uuid") shouldBe "45e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("channel") shouldBe "WEB"
            fst.getAs[String]("schoolId") shouldBe "45e4949d-7eff-4707-9201-dac917a5e013"
          }
        }
      )
    }
  }
}
