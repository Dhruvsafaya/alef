package com.alefeducation.bigdata.streaming.ccl

import com.alefeducation.CCLEvents
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.bigdata.streaming.ccl.testuils.CommonSpec
import com.alefeducation.schema.Schema.{categoryCreatedSchema, categoryDeletedSchema, categoryUpdatedSchema, guardianAssociationSchema, guardianInvitedRegisteredEventSchema, guardiansDeletedEventSchema}

class GuardianEventsTests extends SparkSuite with CommonSpec {

  trait Setup {
    implicit val transformer: CCLEvents = new CCLEvents(spark)
  }


  test("GuardianRegisteredEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-source",
          value = buildKafkaEvent("""
                                    |{"headers":{"eventType":"GuardianRegistered","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"},"body":{"occurredOn":1565182528360,"uuid":"707fc70f-7e7d-4b0e-8232-289f4a63f67a"}}
            """.stripMargin)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          testSink(sinks, "ccl-guardian-sink", guardianInvitedRegisteredEventSchema)
        }
      )
    }
  }

  test("GuardianDeletedEvent") {

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-source",
          value = buildKafkaEvent("""
                           {"headers":{"eventType":"GuardianDeleted","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"},"body":{"events":[{"uuid":"guardian-1","occurredOn":1565182528360}]}}
            """.stripMargin)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          testSink(sinks, "ccl-guardian-deleted-sink", guardiansDeletedEventSchema)
        }
      )
    }
  }

  test("GuardianAssociationEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-source",
          value = buildKafkaEvent("""
                           {"headers":{"eventType":"GuardianAssociationsUpdated","tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"},"body":{"occurredOn":1565182528360,"studentId":"student-id","guardians":[{"id":"guardian-id-2"}]}}
            """.stripMargin)
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          testSink(sinks, "ccl-guardian-association-sink", guardianAssociationSchema)
        }
      )
    }
  }
}
