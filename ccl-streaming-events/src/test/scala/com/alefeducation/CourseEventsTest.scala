package com.alefeducation


import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.bigdata.streaming.ccl.testuils.CommonSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.SortedSet

class CourseEventsTest extends SparkSuite with CommonSpec with Matchers {

  trait Setup {
    implicit val transformer: CourseEvents = new CourseEvents(spark)
  }

  test("should consume DraftCourseCreatedEvent") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value": {
              |	"id": "fda4e1e4-c407-4425-a615-a65016921c92",
              |	"name": "Course1 Course",
              |	"code": "1C001",
              |	"organisation": "shared",
              |	"langCode": "EN_US",
              |	"courseVersion": "1.0",
              |	"courseStatus": "DRAFT",
              |	"courseType": "CORE",
              | "configuration": {
              |     "programEnabled": true,
              |     "resourcesEnabled": false,
              |		  "placementType": "BY_ABILITY_TEST"
              |   },
              |	"eventType": "DraftCourseCreatedEvent",
              |	"occurredOn": 1707075560283
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "DraftCourseCreatedEvent"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-course-draft-created-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-course-draft-created-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "eventType", "eventDateDw", "loadtime", "id", "courseType", "name", "code", "organisation", "occurredOn",
            "langCode", "_headers",  "courseVersion", "configuration", "curriculums"
          )
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume InReviewCourseEvent") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value": {
              |	"id": "245ff2a6-ee07-4a8f-bc4e-d17f0cb4776d",
              |	"organisation": "shared",
              |	"name": "Marcel Test 2024-Copy-1-Copy-1",
              |	"code": "MT204-1-1",
              |	"description": null,
              |	"goal": null,
              |	"langCode": "EN_US",
              |	"subjectIds": [],
              |	"gradeIds": [],
              |	"curriculums": [],
              | "configuration": {
              |     "programEnabled": true,
              |     "resourcesEnabled": false,
              |		  "placementType": "BY_ABILITY_TEST"
              |   },
              |	"courseVersion": "1.0",
              |	"courseStatus": "IN_REVIEW",
              |	"courseType": "PATHWAY",
              |	"eventType": "InReviewCourseEvent",
              |	"occurredOn": 1707063409624
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "InReviewCourseEvent"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-course-in-review-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-course-in-review-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "organisation", "_headers", "eventType", "name", "eventDateDw", "courseType", "description", "loadtime",
            "goal", "code", "occurredOn", "langCode", "id", "gradeIds", "subjectIds", "curriculums", "configuration", "courseVersion"
          )
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume CoursePublishedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              | "id": "377c97b4-2b4a-457b-b820-be38a8443a21",
              |	"type": "CORE",
              |	"organisation": "shared",
              |	"name": "event_test-Copy-1",
              |	"createdOn": "2024-02-02T06:49:15.326",
              |	"code": "event_test-1",
              |	"subOrganisations": [
              |		"6cb68040-b187-4ff6-aa5e-7fca0f1f83f0"
              |	],
              |	"subjectId": null,
              |	"description": null,
              |	"goal": null,
              | "configuration": {
              |     "programEnabled": true,
              |     "resourcesEnabled": false,
              |		  "placementType": "BY_ABILITY_TEST"
              |   },
              |	"modules": [
              |		{
              |			"id": "b794c190-69c4-413d-b8a5-813cf902178b",
              |			"maxAttempts": 1,
              |			"activityId": {
              |				"uuid": "fc2a5a66-6e3a-47a3-822a-000000028930",
              |				"id": 28930
              |			},
              |			"settings": {
              |				"pacing": "LOCKED",
              |				"hideWhenPublishing": false,
              |				"isOptional": false
              |			}
              |		}
              |	],
              |	"langCode": "EN_US",
              |	"subjectIds": [],
              |	"gradeIds": [],
              |	"curriculums": [],
              |	"courseVersion": "1.0",
              |	"courseStatus": "PUBLISHED",
              |	"courseType": "CORE",
              |	"eventType": "CoursePublishedEvent",
              | "instructionalPlanIds": ["1"],
              |	"occurredOn": 1706856698724
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "CoursePublishedEvent"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-course-published-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-course-published-sink is not found"))

          val expectedColumns: SortedSet[String] = SortedSet(
            "eventType", "eventDateDw", "loadtime", "id", "courseType", "organisation", "name", "createdOn",
            "instructionalPlanIds", "configuration", "code", "subOrganisations", "subjectIds", "gradeIds", "description", "goal", "modules", "occurredOn", "langCode", "_headers", "curriculums",
            "courseVersion"
          )
          SortedSet(sink.columns: _*) shouldBe expectedColumns
        }
      )
    }
  }

  test("handle CourseDeletedEvent") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value": {
              |	  "id": "779b3fa1-a037-4220-9efe-60cc0bb3e320",
              |	  "status": "DRAFT",
              |	  "occurredOn": 1666193574364,
              |   "courseType": "PATHWAY"
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "CourseDeletedEvent"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-course-deleted-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-course-deleted-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "eventType", "eventDateDw", "loadtime", "id", "courseType", "status", "occurredOn", "_headers", "courseStatus"
          )
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume CourseSettingsUpdatedEvent") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value": {
              |	  "id": "377c97b4-2b4a-457b-b820-be38a8443a21",
              |	  "type": "CORE",
              |	  "name": "event_test-Copy-1-11",
              |	  "code": "event_test-1",
              |	  "langCode": "EN_US",
              |	  "curriculums": [],
              |   "configuration": {
              |     "programEnabled": true,
              |     "resourcesEnabled": false,
              |		  "placementType": "BY_ABILITY_TEST"
              |   },
              | 	"gradeIds": [],
              |	  "subjectIds": [],
              |	  "courseVersion": "3.0",
              |	  "courseStatus": "PUBLISHED",
              |	  "courseType": "CORE",
              |	  "eventType": "CourseSettingsUpdatedEvent",
              |	  "occurredOn": 1707123231220
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "CourseSettingsUpdatedEvent"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-course-settings-updated-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-course-settings-updated-sin is not found"))

          val expectedColumns: Set[String] = Set(
            "eventType", "eventDateDw", "loadtime", "id", "status", "type", "courseType", "name", "code",
            "subjectIds", "gradeIds", "occurredOn", "langCode", "_headers", "courseStatus", "curriculums", "configuration",
            "courseVersion"
          )
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume AbilityTestComponentEnabledEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"id": "afb3e65f-411c-4544-8ed4-e0468a5fd21b",
              | "type": "DIAGNOSTIC_TEST",
              |	"courseId": "afb3e65f-411c-4544-8ed4-e0468a5fd21b",
              |	"courseType": "PATHWAY",
              |	"courseVersion": "3.0",
              |	"courseStatus": "PUBLISHED",
              |	"occurredOn": 1672923269302
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "AbilityTestComponentEnabledEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      val sinkName = "ccl-published-ability-test-enabled-in-course-sink"
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == sinkName)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$sinkName is not found"))

          val expectedColumns: SortedSet[String] = SortedSet(
            "eventType", "eventDateDw", "loadtime", "occurredOn", "_headers",
            "id",
            "type",
            "courseId",
            "courseType",
            "courseVersion",
            "courseStatus",
          )
          SortedSet(sink.columns: _*) shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume AbilityTestComponentDisabledEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"id": "afb3e65f-411c-4544-8ed4-e0468a5fd21b",
              | "type": "DIAGNOSTIC_TEST",
              |	"courseId": "afb3e65f-411c-4544-8ed4-e0468a5fd21b",
              |	"courseType": "PATHWAY",
              |	"courseVersion": "3.0",
              |	"courseStatus": "PUBLISHED",
              |	"occurredOn": 1672923269302
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "AbilityTestComponentDisabledEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      val sinkName = "ccl-published-ability-test-disabled-in-course-sink"
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == sinkName)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$sinkName is not found"))

          val expectedColumns: SortedSet[String] = SortedSet(
            "eventType", "eventDateDw", "loadtime", "occurredOn", "_headers",
            "id",
            "type",
            "courseId",
            "courseType",
            "courseVersion",
            "courseStatus",
          )
          SortedSet(sink.columns: _*) shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume ActivityPlannedInAbilityTestComponentEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"activityId": {
              |		"uuid": "bccfaa8b-89ca-40a6-9622-000000028439",
              |		"id": 28439
              |	},
              |	"courseId": "afb3e65f-411c-4544-8ed4-e0468a5fd21b",
              |	"courseType": "PATHWAY",
              |	"parentItemId": "d4d2f7d9-a6f1-4b7a-bc22-4d410aed99bd",
              |	"index": -1,
              |	"courseVersion": "3.0",
              |	"courseStatus": "PUBLISHED",
              |	"parentItemType": "DIAGNOSTIC_TEST",
              |	"occurredOn": 1672923269302
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "ActivityPlannedInAbilityTestComponentEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-activity-planned-in-ability-test-component-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-activity-planned-in-ability-test-component-in-course-sink is not found"))

          val expectedColumns: SortedSet[String] = SortedSet("eventType", "eventDateDw", "courseType", "courseId", "parentItemType", "loadtime", "activityId", "maxAttempts", "occurredOn", "parentItemId", "courseVersion", "courseStatus", "settings", "index", "_headers")
          SortedSet(sink.columns: _*) shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume ActivityUnPlannedInAbilityTestComponentEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"activityId": {
              |		"uuid": "bccfaa8b-89ca-40a6-9622-000000028439",
              |		"id": 28439
              |	},
              |	"courseId": "afb3e65f-411c-4544-8ed4-e0468a5fd21b",
              |	"courseType": "PATHWAY",
              |	"parentItemId": "d4d2f7d9-a6f1-4b7a-bc22-4d410aed99bd",
              |	"index": -1,
              |	"courseVersion": "3.0",
              |	"courseStatus": "PUBLISHED",
              |	"parentItemType": "DIAGNOSTIC_TEST",
              |	"occurredOn": 1672923269302
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "ActivityUnPlannedInAbilityTestComponentEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-activity-unplanned-in-ability-test-component-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-activity-unplanned-in-ability-test-component-in-course-sink is not found"))

          val expectedColumns: SortedSet[String] = SortedSet("eventType", "eventDateDw", "courseType", "courseId", "parentItemType", "loadtime", "activityId", "occurredOn", "parentItemId", "courseVersion", "courseStatus", "index", "_headers")
          SortedSet(sink.columns: _*) shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume TestPlannedInAbilityTestComponentEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              | "id": "bccfaa8b-89ca-40a6-9622-000000028439",
              | "legacyId": 28439,
              | "type": "TEST_ACTIVITY",
              | "maxAttempts": 1,
              | "isPlacementTest": false,
              |	"courseId": "afb3e65f-411c-4544-8ed4-e0468a5fd21b",
              |	"courseType": "PATHWAY",
              |	"parentComponentId": "d4d2f7d9-a6f1-4b7a-bc22-4d410aed99bd",
              |	"index": 0,
              |	"courseVersion": "3.0",
              |	"courseStatus": "PUBLISHED",
              |	"parentComponentType": "DIAGNOSTIC_TEST",
              | "metadata": {
              |       "version": "1.0",
              |		  "tags": [
              |			  {
              |				  "key": "Grade",
              |				  "values": [
              |					  "KG",
              |					  "1"
              |				  ],
              |				  "type": "LIST"
              |			  },
              |			  {
              |				  "key": "Domain",
              |				  "values": [
              |					  {
              |						  "name": "Geometry",
              |						  "icon": "Geometry",
              |						  "color": "lightBlue"
              |					  },
              |					  {
              |						  "name": "Algebra and Algebraic Thinking",
              |						  "icon": "AlgebraAndAlgebraicThinking",
              |						  "color": "lightGreen"
              |					  }
              |				  ],
              |				  "type": "DOMAIN"
              |			  }
              |		  ]
              |	  },
              |	"occurredOn": 1672923269302
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "TestPlannedInAbilityTestComponentEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-test-planned-in-ability-test-component-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-test-planned-in-ability-test-component-in-course-sink is not found"))

          val expectedColumns: SortedSet[String] = SortedSet(
            "eventType",
            "eventDateDw",
            "courseType",
            "courseId",
            "parentComponentId",
            "loadtime",
            "id",
            "legacyId",
            "type",
            "maxAttempts",
            "isPlacementTest",
            "occurredOn",
            "parentComponentType",
            "courseVersion",
            "courseStatus",
            "settings",
            "index",
            "metadata",
            "_headers"
          )
          SortedSet(sink.columns: _*) shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume TestUnPlannedInAbilityTestComponentEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              | "id": "bccfaa8b-89ca-40a6-9622-000000028439",
              | "legacyId": 28439,
              | "type": "TEST_ACTIVITY",
              | "maxAttempts": 1,
              | "isPlacementTest": false,
              |	"courseId": "afb3e65f-411c-4544-8ed4-e0468a5fd21b",
              |	"courseType": "PATHWAY",
              |	"parentComponentId": "d4d2f7d9-a6f1-4b7a-bc22-4d410aed99bd",
              |	"index": 0,
              |	"courseVersion": "3.0",
              |	"courseStatus": "PUBLISHED",
              |	"parentComponentType": "DIAGNOSTIC_TEST",
              |	"occurredOn": 1672923269302
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "TestUnPlannedInAbilityTestComponentEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-test-unplanned-in-ability-test-component-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException
            ("ccl-published-test-unplanned-in-ability-test-component-in-course-sink is not found"))

          val expectedColumns: SortedSet[String] = SortedSet(
            "eventType",
            "eventDateDw",
            "courseType",
            "courseId",
            "parentComponentId",
            "loadtime",
            "id",
            "legacyId",
            "type",
            "maxAttempts",
            "isPlacementTest",
            "occurredOn",
            "parentComponentType",
            "courseVersion",
            "courseStatus",
            "settings",
            "index",
            "metadata",
            "_headers"
          )
          SortedSet(sink.columns: _*) shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume TestUpdatedInAbilityTestComponentEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              | "id": "bccfaa8b-89ca-40a6-9622-000000028439",
              | "legacyId": 28439,
              | "type": "TEST_ACTIVITY",
              | "maxAttempts": 1,
              | "isPlacementTest": false,
              |	"courseId": "afb3e65f-411c-4544-8ed4-e0468a5fd21b",
              |	"courseType": "PATHWAY",
              |	"parentComponentId": "d4d2f7d9-a6f1-4b7a-bc22-4d410aed99bd",
              |	"index": 0,
              |	"courseVersion": "3.0",
              |	"courseStatus": "PUBLISHED",
              |	"parentComponentType": "DIAGNOSTIC_TEST",
              |	"occurredOn": 1672923269302
              |},
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "TestUpdatedInAbilityTestComponentEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-01 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-test-updated-in-ability-test-component-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException
            ("ccl-published-test-updated-in-ability-test-component-in-course-sink is not found"))

          val expectedColumns: SortedSet[String] = SortedSet(
            "eventType",
            "eventDateDw",
            "courseType",
            "courseId",
            "parentComponentId",
            "loadtime",
            "id",
            "legacyId",
            "type",
            "maxAttempts",
            "isPlacementTest",
            "occurredOn",
            "parentComponentType",
            "courseVersion",
            "courseStatus",
            "settings",
            "index",
            "metadata",
            "_headers"
          )
          SortedSet(sink.columns: _*) shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume AbilityTestComponentUpdatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
{
 "key": "key1",
 "value": {
	"activityId": {
		"uuid": "bccfaa8b-89ca-40a6-9622-000000028439",
		"id": 28439
	},
	"courseId": "afb3e65f-411c-4544-8ed4-e0468a5fd21b",
	"courseType": "PATHWAY",
	"parentItemId": "d4d2f7d9-a6f1-4b7a-bc22-4d410aed99bd",
	"index": -1,
	"courseVersion": "3.0",
	"courseStatus": "PUBLISHED",
	"parentItemType": "DIAGNOSTIC_TEST",
	"occurredOn": 1672923269302
},
 "headers": [
     {
       "key": "eventType",
       "value": "AbilityTestComponentUpdatedEvent"
     },
     {
       "key": "COURSE_STATUS",
       "value": "PUBLISHED"
     }
  ],
 "timestamp": "2022-09-01 16:23:46.609"
}
]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-activity-updated-in-ability-test-component-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-activity-updated-in-ability-test-component-in-course-sink is not found"))

          val expectedColumns: SortedSet[String] = SortedSet("eventType", "eventDateDw", "courseType", "courseId", "parentItemType", "loadtime", "activityId", "occurredOn", "parentItemId", "courseVersion", "courseStatus", "index", "_headers", "maxAttempts", "settings")
          SortedSet(sink.columns: _*) shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume ContainerPublishedWithCourseEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              | "id": "9bbece95-0e1a-4db5-ae55-d82b790c0b8f",
              | "type": "UNIT",
              |	"courseId": "377c97b4-2b4a-457b-b820-be38a8443a21",
              |	"index": 0,
              |	"title": "Unit-1",
              |	"settings": {
              |		"pacing": "SELF_PACED",
              |		"hideWhenPublishing": true,
              |		"isOptional": false
              |	},
              |	"items": [
              |		{
              |			"id": "da1c7234-03ac-41a8-a538-000000028920",
              |			"settings": {
              |				"pacing": "UN_LOCKED",
              |				"hideWhenPublishing": false,
              |				"isOptional": false
              |			},
              |			"type": "ACTIVITY",
              |			"mappedLearningOutcomes": [],
              |			"isJointParentActivity": false
              |		},
              |		{
              |			"id": "2aca7f89-9584-4a36-8eb7-000000028933",
              |			"settings": {
              |				"pacing": "UN_LOCKED",
              |				"hideWhenPublishing": false,
              |				"isOptional": false
              |			},
              |			"type": "ACTIVITY",
              |			"mappedLearningOutcomes": [],
              |			"isJointParentActivity": false
              |		},
              |		{
              |			"id": "c7fb6a8c-dfef-4c47-b20c-000000028922",
              |			"settings": {
              |				"pacing": "UN_LOCKED",
              |				"hideWhenPublishing": false,
              |				"isOptional": false
              |			},
              |			"type": "ACTIVITY",
              |			"mappedLearningOutcomes": [],
              |			"isJointParentActivity": false
              |		},
              |		{
              |			"id": "a4ff9166-b827-412e-a9dc-000000028924",
              |			"settings": {
              |				"pacing": "UN_LOCKED",
              |				"hideWhenPublishing": false,
              |				"isOptional": false
              |			},
              |			"type": "ACTIVITY",
              |			"mappedLearningOutcomes": [],
              |			"isJointParentActivity": false
              |		}
              |	],
              |	"longName": "Unit-1",
              |	"description": "",
              |	"metadata": {
              |		"tags": []
              |	},
              |	"sequences": [
              |		{
              |			"domain": null,
              |			"sequence": 1
              |		}
              |	],
              |	"courseVersion": "1.0",
              |	"courseStatus": "PUBLISHED",
              |	"courseType": "CORE",
              |	"eventType": "ContainerPublishedWithCourseEvent",
              |	"occurredOn": 1706856698750
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "ContainerPublishedWithCourseEvent"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-container-published-with-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-container-published-with-course-sink is not found"))

          val expectedColumns: Set[String] = Set(
            "eventType", "eventDateDw", "loadtime", "id", "courseId", "courseType", "index", "title", "settings", "items", "longName", "type", "description", "metadata", "sequences", "courseVersion", "occurredOn", "isAccelerated", "_headers"
          )
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume ContainerDeletedFromCourseEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |  "id": "9bbece95-0e1a-4db5-ae55-d82b790c0b8f",
              |	"type": "UNIT",
              |	"courseId": "377c97b4-2b4a-457b-b820-be38a8443a21",
              |	"index": 0,
              |	"courseVersion": "2.0",
              |	"courseStatus": "PUBLISHED",
              |	"courseType": "CORE",
              |	"eventType": "ContainerDeletedFromCourseEvent",
              |	"occurredOn": 1707113591264
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "ContainerDeletedFromCourseEvent"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-container-deleted-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-container-deleted-in-course-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "loadtime", "courseId", "courseType", "occurredOn", "id", "type", "courseVersion", "courseStatus", "index", "_headers")
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume ContainerUpdatedInCourseEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              | "id": "dd2d3f4d-04f7-4662-bdd2-ea30c3bbcaff",
              |	"type": "UNIT",
              |	"courseId": "377c97b4-2b4a-457b-b820-be38a8443a21",
              |	"index": 5,
              |	"title": "Unit-2",
              |	"settings": {
              |		"pacing": "SELF_PACED",
              |		"hideWhenPublishing": true,
              |		"isOptional": true
              |	},
              |	"longName": "1",
              |	"description": "",
              |	"metadata": {
              |		"tags": [
              |			{
              |				"key": "Grade",
              |				"values": [
              |					"KG"
              |				],
              |				"attributes": [
              |					{
              |						"value": "KG",
              |						"color": "lightGray",
              |						"translation": null
              |					}
              |				],
              |				"type": "LIST"
              |			}
              |		]
              |	},
              |	"sequences": [],
              |	"isAccelerated": null,
              |	"courseVersion": "3.0",
              |	"courseStatus": "IN_REVIEW",
              |	"courseType": "CORE",
              |	"eventType": "ContainerUpdatedInCourseEvent",
              |	"occurredOn": 1707120522564
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "ContainerUpdatedInCourseEvent"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-container-updated-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-container-updated-in-course-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "description", "loadtime", "courseId", "courseType", "occurredOn", "sequences", "id", "metadata", "courseVersion", "title",  "type", "courseStatus", "longName", "settings", "index", "isAccelerated", "_headers")
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume ContainerAddedInCourseEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              |"key": "key1",
              | "value": {
              | "id": "00139d80-e1f3-4c7e-ab65-9b8c1c8cdc87",
              |	"type": "UNIT",
              |	"courseId": "377c97b4-2b4a-457b-b820-be38a8443a21",
              |	"index": 4,
              |	"title": "Unit-3",
              |	"settings": {
              |		"pacing": "SELF_PACED",
              |		"hideWhenPublishing": true,
              |		"isOptional": false
              |	},
              |	"longName": "Geometry Part 1",
              |	"description": "desc",
              |	"metadata": {
              |		"tags": [
              |			{
              |				"key": "Grade",
              |				"values": [
              |					"KG"
              |				],
              |				"attributes": [
              |					{
              |						"value": "KG",
              |						"color": "lightGray",
              |						"translation": null
              |					}
              |				],
              |				"type": "LIST"
              |			}
              |		]
              |	},
              |	"sequences": [],
              |	"isAccelerated": null,
              |	"courseVersion": "3.0",
              |	"courseStatus": "PUBLISHED",
              |	"courseType": "CORE",
              |	"eventType": "ContainerAddedInCourseEvent",
              |	"occurredOn": 1707120522564
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "ContainerAddedInCourseEvent"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-container-added-in-course-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-container-added-in-course-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "description", "loadtime", "courseId", "courseType",
            "occurredOn", "sequences", "id", "metadata", "courseVersion", "title", "type", "courseStatus", "longName",
            "settings", "index", "isAccelerated", "_headers")
          sink.columns.toSet shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume course instructionalPlanPublishedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |	{
              |		"key": "key1",
              |		"value": {
              |			"id": "36e8f683-46a2-4b60-9064-dec21b1893af",
              |			"courseId": "0c0baaa1-c336-460e-86e7-c6b89e79a890",
              |			"timeFrames": [
              |				{
              |					"id": "45a57ac5-db32-4b14-920c-73786a16bbcc",
              |					"items": [
              |						{
              |							"parentContainer": {
              |								"id": "af750b05-a414-4931-a660-f494812b30fa",
              |								"title": "Unit-1"
              |							},
              |							"activity": {
              |								"id": "5bc64923-c4af-49ef-ad05-000000028857",
              |								"settings": {
              |									"pacing": "UN_LOCKED",
              |									"hideWhenPublishing": false,
              |									"isOptional": false
              |								},
              |								"type": "ACTIVITY",
              |								"mappedLearningOutcomes": [
              |									{
              |										"id": 14973,
              |										"type": "SKILL",
              |										"curriculumId": 392027,
              |										"gradeId": 596550,
              |										"subjectId": 571671
              |									}
              |								],
              |								"isJointParentActivity": false
              |							}
              |						}
              |					]
              |				}
              |			],
              |			"courseVersion": "1.0",
              |			"courseType": "CORE",
              |			"courseStatus": "IN_REVIEW",
              |			"eventType": "InstructionalPlanPublishedEvent",
              |			"occurredOn": 1709534545801
              |		},
              |		"headers": [
              |			{
              |				"key": "eventType",
              |				"value": "InstructionalPlanPublishedEvent"
              |			}
              |		],
              |		"timestamp": "2022-09-27 16:23:46.609"
              |	}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-course-instructional-plan-published-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-course-instructional-plan-published-sink is not found"))

          val expectedColumns = SortedSet(
            "eventType", "eventDateDw", "loadtime", "occurredOn", "_headers",
            "id",
            "courseId",
            "timeFrames",
            "courseVersion",
            "courseType",
            "courseStatus",
          )
          SortedSet(sink.columns: _*) shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume course InstructionalPlanInReviewEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |	{
              |		"key": "key1",
              |		"value": {
              |			"id": "36e8f683-46a2-4b60-9064-dec21b1893af",
              |			"courseId": "0c0baaa1-c336-460e-86e7-c6b89e79a890",
              |			"timeFrames": [
              |				{
              |					"id": "45a57ac5-db32-4b14-920c-73786a16bbcc",
              |					"items": [
              |						{
              |							"parentContainer": {
              |								"id": "af750b05-a414-4931-a660-f494812b30fa",
              |								"title": "Unit-1"
              |							},
              |							"activity": {
              |								"id": "5bc64923-c4af-49ef-ad05-000000028857",
              |								"settings": {
              |									"pacing": "UN_LOCKED",
              |									"hideWhenPublishing": false,
              |									"isOptional": false
              |								},
              |								"type": "ACTIVITY",
              |								"mappedLearningOutcomes": [
              |									{
              |										"id": 14973,
              |										"type": "SKILL",
              |										"curriculumId": 392027,
              |										"gradeId": 596550,
              |										"subjectId": 571671
              |									}
              |								],
              |								"isJointParentActivity": false
              |							}
              |						}
              |					]
              |				}
              |			],
              |			"courseVersion": "1.0",
              |			"courseType": "CORE",
              |			"courseStatus": "IN_REVIEW",
              |			"eventType": "InstructionalPlanInReviewEvent",
              |			"occurredOn": 1709534545801
              |		},
              |		"headers": [
              |			{
              |				"key": "eventType",
              |				"value": "InstructionalPlanInReviewEvent"
              |			}
              |		],
              |		"timestamp": "2022-09-27 16:23:46.609"
              |	}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-course-instructional-plan-in-review-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-course-instructional-plan-in-review-sink is not found"))

          val expectedColumns = SortedSet(
            "eventType", "eventDateDw", "loadtime", "occurredOn", "_headers",
            "id",
            "courseId",
            "timeFrames",
            "courseVersion",
            "courseType",
            "courseStatus",
          )
          SortedSet(sink.columns: _*) shouldBe expectedColumns
        }
      )
    }
  }

  test("should consume course InstructionalPlanDeletedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |	{
              |		"key": "key1",
              |		"value": {
              |			"id": "36e8f683-46a2-4b60-9064-dec21b1893af",
              |			"courseId": "0c0baaa1-c336-460e-86e7-c6b89e79a890",
              |			"courseVersion": "1.0",
              |			"courseType": "CORE",
              |			"courseStatus": "IN_REVIEW",
              |			"eventType": "InstructionalPlanDeletedEvent",
              |			"occurredOn": 1709534545801
              |		},
              |		"headers": [
              |			{
              |				"key": "eventType",
              |				"value": "InstructionalPlanDeletedEvent"
              |			}
              |		],
              |		"timestamp": "2022-09-27 16:23:46.609"
              |	}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val sink = sinks
            .find(_.name == "ccl-published-course-instructional-plan-deleted-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-course-instructional-plan-deleted-sink is not found"))

          val expectedColumns = SortedSet(
            "eventType", "eventDateDw", "loadtime", "occurredOn", "_headers",
            "id",
            "courseId",
            "courseVersion",
            "courseType",
            "courseStatus",
          )
          SortedSet(sink.columns: _*) shouldBe expectedColumns
        }
      )
    }
  }


  test("should consume DownloadableResourcePlannedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              | "courseType": "PATHWAY",
              |	"courseVersion": "3.0",
              |	"courseStatus": "PUBLISHED",
              |	"courseId": "a54fb055-baa6-4f4d-9695-8082544f2e9f",
              |	"resourceId": "a396ec53-26c0-4830-b40c-4594bb1f5f7b",
              |	"description": "lorem",
              |	"metadata": {
              |		"tags": []
              |	},
              |	"fileInfo": {
              |		"fileName": "test1.pdf",
              |		"fileId": "abc06a41-6d15-49f3-a71d-73edca8499d8",
              |		"path": "authoring-courses/abc06a41-6d15-49f3-a71d-73edca8499d8.pdf"
              |	},
              |	"type": "PDF_RESOURCE",
              |	"title": "lorem",
              |	"eventType": "DownloadableResourcePlannedEvent",
              |	"occurredOn": 1721132641906
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "DownloadableResourcePlannedEvent"
              |     },
              |     {
              |       "key": "EVENTS_SOURCE",
              |       "value": "REAL_TIME"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |},
              |{
              | "key": "key2",
              | "value": {
              | "courseType": "PATHWAY",
              |	"courseVersion": "1.0",
              |	"courseStatus": "IN_REVIEW",
              |	"courseId": "b48cdc50-532d-4604-add7-cb71457540c8",
              |	"resourceId": "d05d4b69-8d5c-4a09-ac18-674f14e4b690",
              |	"description": "dfg",
              |	"metadata": {
              |		"tags": [
              |			{
              |				"key": "Grade",
              |				"values": [
              |					"KG"
              |				],
              |				"attributes": [
              |					{
              |						"value": "KG",
              |						"color": "lightGray",
              |						"translation": null
              |					}
              |				],
              |				"type": "LIST"
              |			},
              |			{
              |				"key": "Topic",
              |				"values": [
              |					"Dummy Topic"
              |				],
              |				"attributes": [
              |					{
              |						"value": "Dummy Topic",
              |						"color": "lightGray",
              |						"translation": null
              |					}
              |				],
              |				"type": "LIST"
              |			},
              |			{
              |				"key": "Domain",
              |				"values": [
              |					{
              |						"name": "Geometry",
              |						"icon": "Geometry",
              |						"color": "lightBlue"
              |					}
              |				],
              |				"type": "DOMAIN"
              |			}
              |		]
              |	},
              |	"fileInfo": {
              |		"fileName": "dummy-pdf_2.pdf",
              |		"fileId": "a3726595-8594-4f7a-acdb-ac2d812dd8fd",
              |		"path": "authoring-courses/a3726595-8594-4f7a-acdb-ac2d812dd8fd.pdf"
              |	},
              |	"type": "PDF_RESOURCE",
              |	"title": "dummy pdf",
              |	"eventType": "DownloadableResourcePlannedEvent",
              |	"occurredOn": 1721198809263
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "DownloadableResourcePlannedEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "IN_REVIEW"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val publishedSink = sinks
            .find(_.name == "ccl-published-downloadable-resource-planned-in-courses-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-downloadable-resource-planned-in-courses-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "courseType", "title", "courseId",
            "loadtime", "resourceId", "occurredOn", "courseVersion", "courseStatus", "description", "type",
            "fileInfo", "_headers", "metadata")
          publishedSink.toDF().count() shouldBe 1
          publishedSink.columns.toSet shouldBe expectedColumns

          val inReviewSink = sinks
            .find(_.name == "ccl-in-review-downloadable-resource-planned-in-courses-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-in-review-downloadable-resource-planned-in-courses-sink is not found"))

          inReviewSink.toDF().count() shouldBe 1
        }
      )
    }
  }

  test("should consume AdditionalResourceActivityPlannedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              | "courseType": "PATHWAY",
              |	"courseVersion": "1.0",
              |	"courseStatus": "PUBLISHED",
              |	"courseId": "b48cdc50-532d-4604-add7-cb71457540c8",
              |	"resourceId": "8d9f2464-fe0c-4f3e-bd69-000000030197",
              |	"type": "ACTIVITY",
              |	"legacyId": 30197,
              |	"mappedLearningOutcomes": [],
              |	"metadata": {
              |		"tags": []
              |	},
              |	"eventType": "AdditionalResourceActivityPlannedEvent",
              |	"occurredOn": 1721198809263
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "AdditionalResourceActivityPlannedEvent"
              |     },
              |     {
              |       "key": "EVENTS_SOURCE",
              |       "value": "REAL_TIME"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |},
              |{
              | "key": "key2",
              | "value": {
              | "courseType": "PATHWAY",
              |	"courseVersion": "1.0",
              |	"courseStatus": "IN_REVIEW",
              |	"courseId": "b48cdc50-532d-4604-add7-cb71457540c8",
              |	"resourceId": "020c776c-9f8f-42da-b290-000000029387",
              |	"type": "ACTIVITY",
              |	"legacyId": 29387,
              |	"mappedLearningOutcomes": [],
              |	"metadata": {
              |		"tags": []
              |	},
              |	"eventType": "AdditionalResourceActivityPlannedEvent",
              |	"occurredOn": 1721198809263
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "AdditionalResourceActivityPlannedEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "IN_REVIEW"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val publishedSink = sinks
            .find(_.name == "ccl-published-additional-resource-activity-planned-in-courses-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-additional-resource-activity-planned-in-courses-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "courseType", "courseId", "loadtime",
            "resourceId", "occurredOn", "courseVersion", "courseStatus", "type", "_headers",
            "mappedLearningOutcomes", "legacyId", "metadata")
          publishedSink.toDF().count() shouldBe 1
          publishedSink.columns.toSet shouldBe expectedColumns

          val inReviewSink = sinks
            .find(_.name == "ccl-in-review-additional-resource-activity-planned-in-courses-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-in-review-additional-resource-activity-planned-in-courses-sink is not found"))

          inReviewSink.toDF().count() shouldBe 1
        }
      )
    }
  }

  test("should consume DownloadableResourceUnplannedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"courseType": "PATHWAY",
              |	"courseVersion": "2.0",
              |	"courseStatus": "PUBLISHED",
              |	"courseId": "6f356ec4-5960-4cc6-8b6a-716377d5da31",
              |	"resourceId": "d05d4b69-8d5c-4a09-ac18-674f14e4b690",
              |	"description": "dfg",
              |	"metadata": {
              |		"tags": [
              |			{
              |				"key": "Grade",
              |				"values": [
              |					"KG"
              |				],
              |				"attributes": [
              |					{
              |						"value": "KG",
              |						"color": "lightGray",
              |						"translation": null
              |					}
              |				],
              |				"type": "LIST"
              |			},
              |			{
              |				"key": "Topic",
              |				"values": [
              |					"Dummy Topic"
              |				],
              |				"attributes": [
              |					{
              |						"value": "Dummy Topic",
              |						"color": "lightGray",
              |						"translation": null
              |					}
              |				],
              |				"type": "LIST"
              |			},
              |			{
              |				"key": "Domain",
              |				"values": [
              |					{
              |						"name": "Geometry",
              |						"icon": "Geometry",
              |						"color": "lightBlue"
              |					}
              |				],
              |				"type": "DOMAIN"
              |			}
              |		]
              |	},
              |	"fileInfo": {
              |		"fileName": "dummy-pdf_2.pdf",
              |		"fileId": "a3726595-8594-4f7a-acdb-ac2d812dd8fd",
              |		"path": "authoring-courses/a3726595-8594-4f7a-acdb-ac2d812dd8fd.pdf"
              |	},
              |	"type": "PDF_RESOURCE",
              |	"title": "dummy pdf",
              |	"eventType": "DownloadableResourceUnplannedEvent",
              |	"occurredOn": 1721199830252
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "DownloadableResourceUnplannedEvent"
              |     },
              |     {
              |       "key": "EVENTS_SOURCE",
              |       "value": "REAL_TIME"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |},
              |{
              | "key": "key2",
              | "value": {
              | "courseType": "PATHWAY",
              |	"courseVersion": "2.0",
              |	"courseStatus": "IN_REVIEW",
              |	"courseId": "6f356ec4-5960-4cc6-8b6a-716377d5da31",
              |	"resourceId": "d05d4b69-8d5c-4a09-ac18-674f14e4b690",
              |	"description": "dfg",
              |	"metadata": {
              |		"tags": [
              |			{
              |				"key": "Grade",
              |				"values": [
              |					"KG"
              |				],
              |				"attributes": [
              |					{
              |						"value": "KG",
              |						"color": "lightGray",
              |						"translation": null
              |					}
              |				],
              |				"type": "LIST"
              |			},
              |			{
              |				"key": "Topic",
              |				"values": [
              |					"Dummy Topic"
              |				],
              |				"attributes": [
              |					{
              |						"value": "Dummy Topic",
              |						"color": "lightGray",
              |						"translation": null
              |					}
              |				],
              |				"type": "LIST"
              |			},
              |			{
              |				"key": "Domain",
              |				"values": [
              |					{
              |						"name": "Geometry",
              |						"icon": "Geometry",
              |						"color": "lightBlue"
              |					}
              |				],
              |				"type": "DOMAIN"
              |			}
              |		]
              |	},
              |	"fileInfo": {
              |		"fileName": "dummy-pdf_2.pdf",
              |		"fileId": "a3726595-8594-4f7a-acdb-ac2d812dd8fd",
              |		"path": "authoring-courses/a3726595-8594-4f7a-acdb-ac2d812dd8fd.pdf"
              |	},
              |	"type": "PDF_RESOURCE",
              |	"title": "dummy pdf",
              |	"eventType": "DownloadableResourceUnplannedEvent",
              |	"occurredOn": 1721199830252
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "DownloadableResourceUnplannedEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "IN_REVIEW"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val publishedSink = sinks
            .find(_.name == "ccl-published-downloadable-resource-unplanned-in-courses-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-downloadable-resource-unplanned-in-courses-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "courseType", "title", "courseId",
            "loadtime", "resourceId", "occurredOn", "courseVersion", "courseStatus", "description", "type",
            "fileInfo", "_headers", "metadata")
          publishedSink.toDF().count() shouldBe 1
          publishedSink.columns.toSet shouldBe expectedColumns

          val inReviewSink = sinks
            .find(_.name == "ccl-in-review-downloadable-resource-unplanned-in-courses-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-in-review-downloadable-resource-unplanned-in-courses-sink is not found"))

          inReviewSink.toDF().count() shouldBe 1
        }
      )
    }
  }

  test("should consume AdditionalResourceActivityUnPlannedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              | "courseType": "PATHWAY",
              |	"courseVersion": "2.0",
              |	"courseStatus": "IN_REVIEW",
              |	"courseId": "6f356ec4-5960-4cc6-8b6a-716377d5da31",
              |	"resourceId": "8d9f2464-fe0c-4f3e-bd69-000000030197",
              |	"type": "ACTIVITY",
              |	"legacyId": 30197,
              |	"mappedLearningOutcomes": [],
              |	"metadata": {
              |		"tags": []
              |	},
              |	"eventType": "AdditionalResourceActivityUnplannedEvent",
              |	"occurredOn": 1721199830251
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "AdditionalResourceActivityUnplannedEvent"
              |     },
              |     {
              |       "key": "EVENTS_SOURCE",
              |       "value": "REAL_TIME"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "IN_REVIEW"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |},
              |{
              | "key": "key2",
              | "value": {
              |"courseType": "PATHWAY",
              |	"courseVersion": "2.0",
              |	"courseStatus": "PUBLISHED",
              |	"courseId": "6f356ec4-5960-4cc6-8b6a-716377d5da31",
              |	"resourceId": "8d9f2464-fe0c-4f3e-bd69-000000030197",
              |	"type": "ACTIVITY",
              |	"legacyId": 30197,
              |	"mappedLearningOutcomes": [],
              |	"metadata": {
              |		"tags": []
              |	},
              |	"eventType": "AdditionalResourceActivityUnplannedEvent",
              |	"occurredOn": 1721199830251
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "AdditionalResourceActivityUnplannedEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val publishedSink = sinks
            .find(_.name == "ccl-published-additional-resource-activity-unplanned-in-courses-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-additional-resource-activity-unplanned-in-courses-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "courseType", "courseId", "loadtime",
            "resourceId", "occurredOn", "courseVersion", "courseStatus", "type", "_headers",
            "mappedLearningOutcomes", "legacyId", "metadata")
          publishedSink.toDF().count() shouldBe 1
          publishedSink.columns.toSet shouldBe expectedColumns

          val inReviewSink = sinks
            .find(_.name == "ccl-in-review-additional-resource-activity-unplanned-in-courses-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-in-review-additional-resource-activity-unplanned-in-courses-sink is not found"))

          inReviewSink.toDF().count() shouldBe 1
        }
      )
    }
  }

  test("should consume AdditionalResourceActivityUpdatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              | "courseType": "PATHWAY",
              |	"courseVersion": "2.0",
              |	"courseStatus": "IN_REVIEW",
              |	"courseId": "6f356ec4-5960-4cc6-8b6a-716377d5da31",
              |	"resourceId": "8d9f2464-fe0c-4f3e-bd69-000000030197",
              |	"type": "ACTIVITY",
              |	"legacyId": 30197,
              |	"mappedLearningOutcomes": [],
              |	"metadata": {
              |		"tags": []
              |	},
              |	"eventType": "AdditionalResourceActivityUpdatedEvent",
              |	"occurredOn": 1721199830251
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "AdditionalResourceActivityUpdatedEvent"
              |     },
              |     {
              |       "key": "EVENTS_SOURCE",
              |       "value": "REAL_TIME"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "IN_REVIEW"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |},
              |{
              | "key": "key2",
              | "value": {
              |"courseType": "PATHWAY",
              |	"courseVersion": "2.0",
              |	"courseStatus": "PUBLISHED",
              |	"courseId": "6f356ec4-5960-4cc6-8b6a-716377d5da31",
              |	"resourceId": "8d9f2464-fe0c-4f3e-bd69-000000030197",
              |	"type": "ACTIVITY",
              |	"legacyId": 30197,
              |	"mappedLearningOutcomes": [],
              |	"metadata": {
              |		"tags": []
              |	},
              |	"eventType": "AdditionalResourceActivityUpdatedEvent",
              |	"occurredOn": 1721199830251
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "AdditionalResourceActivityUpdatedEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val publishedSink = sinks
            .find(_.name == "ccl-published-additional-resource-activity-updated-in-courses-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-additional-resource-activity-updated-in-courses-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "courseType", "courseId", "loadtime",
            "resourceId", "occurredOn", "courseVersion", "courseStatus", "type", "_headers",
            "mappedLearningOutcomes", "legacyId", "metadata")
          publishedSink.toDF().count() shouldBe 1
          publishedSink.columns.toSet shouldBe expectedColumns

          val inReviewSink = sinks
            .find(_.name == "ccl-in-review-additional-resource-activity-updated-in-courses-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-in-review-additional-resource-activity-updated-in-courses-sink is not found"))

          inReviewSink.toDF().count() shouldBe 1
        }
      )
    }
  }

  test("should consume DownloadableResourceUpdatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "ccl-course-source",
          value =
            """[
              |{
              | "key": "key1",
              | "value": {
              |	"courseType": "PATHWAY",
              |	"courseVersion": "2.0",
              |	"courseStatus": "PUBLISHED",
              |	"courseId": "6f356ec4-5960-4cc6-8b6a-716377d5da31",
              |	"resourceId": "d05d4b69-8d5c-4a09-ac18-674f14e4b690",
              |	"description": "dfg",
              |	"metadata": {
              |		"tags": [
              |			{
              |				"key": "Grade",
              |				"values": [
              |					"KG"
              |				],
              |				"attributes": [
              |					{
              |						"value": "KG",
              |						"color": "lightGray",
              |						"translation": null
              |					}
              |				],
              |				"type": "LIST"
              |			},
              |			{
              |				"key": "Topic",
              |				"values": [
              |					"Dummy Topic"
              |				],
              |				"attributes": [
              |					{
              |						"value": "Dummy Topic",
              |						"color": "lightGray",
              |						"translation": null
              |					}
              |				],
              |				"type": "LIST"
              |			},
              |			{
              |				"key": "Domain",
              |				"values": [
              |					{
              |						"name": "Geometry",
              |						"icon": "Geometry",
              |						"color": "lightBlue"
              |					}
              |				],
              |				"type": "DOMAIN"
              |			}
              |		]
              |	},
              |	"fileInfo": {
              |		"fileName": "dummy-pdf_2.pdf",
              |		"fileId": "a3726595-8594-4f7a-acdb-ac2d812dd8fd",
              |		"path": "authoring-courses/a3726595-8594-4f7a-acdb-ac2d812dd8fd.pdf"
              |	},
              |	"type": "PDF_RESOURCE",
              |	"title": "dummy pdf",
              |	"eventType": "DownloadableResourceUpdatedEvent",
              |	"occurredOn": 1721199830252
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "DownloadableResourceUpdatedEvent"
              |     },
              |     {
              |       "key": "EVENTS_SOURCE",
              |       "value": "REAL_TIME"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "PUBLISHED"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |},
              |{
              | "key": "key2",
              | "value": {
              | "courseType": "PATHWAY",
              |	"courseVersion": "2.0",
              |	"courseStatus": "IN_REVIEW",
              |	"courseId": "6f356ec4-5960-4cc6-8b6a-716377d5da31",
              |	"resourceId": "d05d4b69-8d5c-4a09-ac18-674f14e4b690",
              |	"description": "dfg",
              |	"metadata": {
              |		"tags": [
              |			{
              |				"key": "Grade",
              |				"values": [
              |					"KG"
              |				],
              |				"attributes": [
              |					{
              |						"value": "KG",
              |						"color": "lightGray",
              |						"translation": null
              |					}
              |				],
              |				"type": "LIST"
              |			},
              |			{
              |				"key": "Topic",
              |				"values": [
              |					"Dummy Topic"
              |				],
              |				"attributes": [
              |					{
              |						"value": "Dummy Topic",
              |						"color": "lightGray",
              |						"translation": null
              |					}
              |				],
              |				"type": "LIST"
              |			},
              |			{
              |				"key": "Domain",
              |				"values": [
              |					{
              |						"name": "Geometry",
              |						"icon": "Geometry",
              |						"color": "lightBlue"
              |					}
              |				],
              |				"type": "DOMAIN"
              |			}
              |		]
              |	},
              |	"fileInfo": {
              |		"fileName": "dummy-pdf_2.pdf",
              |		"fileId": "a3726595-8594-4f7a-acdb-ac2d812dd8fd",
              |		"path": "authoring-courses/a3726595-8594-4f7a-acdb-ac2d812dd8fd.pdf"
              |	},
              |	"type": "PDF_RESOURCE",
              |	"title": "dummy pdf",
              |	"eventType": "DownloadableResourceUpdatedEvent",
              |	"occurredOn": 1721199830252
              | },
              | "headers": [
              |     {
              |       "key": "eventType",
              |       "value": "DownloadableResourceUpdatedEvent"
              |     },
              |     {
              |       "key": "COURSE_STATUS",
              |       "value": "IN_REVIEW"
              |     }
              |  ],
              | "timestamp": "2022-09-27 16:23:46.609"
              |}
              |]""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val publishedSink = sinks
            .find(_.name == "ccl-published-downloadable-resource-updated-in-courses-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-published-downloadable-resource-updated-in-courses-sink is not found"))

          val expectedColumns: Set[String] = Set("eventType", "eventDateDw", "courseType", "title", "courseId",
            "loadtime", "resourceId", "occurredOn", "courseVersion", "courseStatus", "description", "type",
            "fileInfo", "_headers", "metadata")
          publishedSink.toDF().count() shouldBe 1
          publishedSink.columns.toSet shouldBe expectedColumns

          val inReviewSink = sinks
            .find(_.name == "ccl-in-review-downloadable-resource-updated-in-courses-sink")
            .map(_.output)
            .getOrElse(throw new RuntimeException("ccl-in-review-downloadable-resource-updated-in-courses-sink is not found"))

          inReviewSink.toDF().count() shouldBe 1
        }
      )
    }
  }


}
