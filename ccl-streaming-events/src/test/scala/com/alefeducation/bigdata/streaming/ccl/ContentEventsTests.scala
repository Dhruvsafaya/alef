package com.alefeducation.bigdata.streaming.ccl

import com.alefeducation.CCLEvents
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.bigdata.streaming.ccl.testuils.CommonSpec
import com.alefeducation.schema.Schema.contentSkillAttachedDetachedSchema

class ContentEventsTests extends SparkSuite with CommonSpec {

  trait Setup {
    implicit val transformer: CCLEvents = new CCLEvents(spark)
  }

  test("ContentSkillAttachedEvent") {
    new Setup {
      val eventsJson =
        """
          |{"headers":{"eventType":"ContentSkillAttachedEvent"},"body":{"contentId":1,"skillId":234523,"occurredOn":1573012390}}
          |""".stripMargin

      val fixtures = List(
        SparkFixture(
          key = "ccl-source",
          value = buildKafkaEvents(eventsJson)
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          testSink(sinks, "ccl-content-skill-attached-detached-sink", contentSkillAttachedDetachedSchema)
        }
      )
    }
  }

  test("ContentSkillDetachedEvent") {
    new Setup {
      val eventsJson =
        s"""
           |{"headers":{"eventType":"ContentSkillDetachedEvent"},"body":{"contentId":1,"skillId":234523,"occurredOn":1573012390}}
           |""".stripMargin

      val fixtures = List(
        SparkFixture(
          key = "ccl-source",
          value = buildKafkaEvents(eventsJson)
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          testSink(sinks, "ccl-content-skill-attached-detached-sink", contentSkillAttachedDetachedSchema)
        }
      )
    }
  }

}
