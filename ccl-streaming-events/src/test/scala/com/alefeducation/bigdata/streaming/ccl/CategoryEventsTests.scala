package com.alefeducation.bigdata.streaming.ccl

import com.alefeducation.CCLEvents
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.bigdata.streaming.ccl.testuils.CommonSpec
import com.alefeducation.schema.Schema.{categoryCreatedSchema, categoryDeletedSchema, categoryUpdatedSchema, guardianAssociationSchema, guardianInvitedRegisteredEventSchema, guardiansDeletedEventSchema}

class CategoryEventsTests extends SparkSuite with CommonSpec {

  trait Setup {
    implicit val transformer: CCLEvents = new CCLEvents(spark)
  }

  test("CategoryCreatedEvent") {
    new Setup {
      val eventsJson =
        """
          |{"headers":{"eventType":"CategoryCreatedEvent"},"body":{"id":"6bbd2ad2-926d-45c0-9ff6-37cb99098150","code":"CTGRY.8","name":"cat04","description":"cat04 desc","occurredOn":1573028231}}
          |""".stripMargin

      val fixtures = List(
        SparkFixture(
          key = "ccl-source",
          value = buildKafkaEvents(eventsJson)
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          testSink(sinks, "ccl-category-created-sink", categoryCreatedSchema)
        }
      )
    }
  }

  test("CategoryUpdatedEvent") {
    new Setup {
      val eventsJson =
        s"""
           |{"headers":{"eventType":"CategoryUpdatedEvent"},"body":{"id":"6bbd2ad2-926d-45c0-9ff6-37cb99098150","name":"cat04","description":"cat04 desc","occurredOn":1573028231}}
           |""".stripMargin

      val fixtures = List(
        SparkFixture(
          key = "ccl-source",
          value = buildKafkaEvents(eventsJson)
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          testSink(sinks, "ccl-category-updated-sink", categoryUpdatedSchema)
        }
      )
    }
  }

  test("CategoryDeletedEvent") {
    new Setup {
      val eventsJson =
        s"""
           |{"headers":{"eventType":"CategoryDeletedEvent"},"body":{"id":"6bbd2ad2-926d-45c0-9ff6-37cb99098150","occurredOn":1573029173}}
           |""".stripMargin

      val fixtures = List(
        SparkFixture(
          key = "ccl-source",
          value = buildKafkaEvents(eventsJson)
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          testSink(sinks, "ccl-category-deleted-sink", categoryDeletedSchema)
        }
      )
    }
  }

}
