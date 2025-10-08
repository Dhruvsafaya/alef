package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.schema.teacherTest.{GuidanceClass, GuidanceLesson}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.scalatest.matchers.should.Matchers

class TeacherTestRawEventsSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer = new TeacherTestRawEvents(spark)
    val TeacherTestIntegrationEventsSource: String = "teacher-test-integration-events-source"
  }

  test("handle TestBlueprintCreatedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "contextFrameTitle",
        "guidanceVersion",
        "aggregateIdentifier",
        "contextDomainId",
        "loadtime",
        "noOfQuestions",
        "contextFrameClassId",
        "occurredOn",
        "id",
        "status",
        "guidanceVariableLessons",
        "createdAt",
        "createdBy",
        "tenantId",
        "contextFrameDescription",
        "guidanceType"
      )
      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"TestBlueprintCreatedIntegrationEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "id": "ef7ffaa2-2dce-41d3-88a2-c4fef0db85cc",
              |       "contextFrameClassId": "58e158bf-020f-45a5-bd94-732cb5c2a91c",
              |       "contextFrameTitle": "test-15",
              |       "contextDomainId": "5ba14edb-464e-4cb9-b5e9-0da24713ac45",
              |       "contextFrameDescription": "test 1",
              |	      "createdBy": "23c3d147-2a78-4bf6-956a-2c4e779c96bb",
              |	      "createdAt": "2024-03-12T09:35:27.355887629",
              |	      "status": "DRAFT",
              |	      "occurredOn": 1710236127,
              |	      "aggregateIdentifier": "ef7ffaa2-2dce-41d3-88a2-c4fef0db85cc"
              |     }
              | },
              | "timestamp": "2023-05-15 16:23:46.609"
              |}
              |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-blueprint-created-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "TestBlueprintCreatedIntegrationEvent"
            fst.getAs[String]("id") shouldBe "ef7ffaa2-2dce-41d3-88a2-c4fef0db85cc"
            fst.getAs[String]("createdBy") shouldBe "23c3d147-2a78-4bf6-956a-2c4e779c96bb"
            fst.getAs[Int]("status") shouldBe "DRAFT"
            fst.getAs[String]("occurredOn") shouldBe "1970-01-20 19:03:56.127"
            fst.getAs[String]("aggregateIdentifier") shouldBe "ef7ffaa2-2dce-41d3-88a2-c4fef0db85cc"
          }
        }
      )
    }
  }

  test("handle TestBlueprintGuidanceUpdatedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "id",
        "guidanceVariableLessons",
        "noOfQuestions",
        "guidanceType",
        "guidanceVersion",
        "occurredOn",
        "aggregateIdentifier",
        "tenantId"
      )
      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"TestBlueprintGuidanceUpdatedIntegrationEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "id": "eff64c0f-f473-41cf-95cd-350dc7acc32a",
              |       "guidanceVariableLessons": [
              |         {
              |           "id": "lesson1",
              |           "classes": [
              |             {
              |               "id": "class1",
              |               "materialType": "CORE",
              |               "materialId": "material123"
              |             },
              |             {
              |               "id": "class2",
              |               "materialType": "PATHWAY",
              |               "materialId": "material456"
              |             }
              |           ]
              |         }
              |       ],
              |       "noOfQuestions": 1,
              |       "guidanceType": "LESSONS_BASED",
              |       "guidanceVersion": {
              |         "major": 1,
              |         "minor": 1,
              |         "revision": 0
              |       },
              |       "occurredOn": 1710243258020,
              |       "aggregateIdentifier": "eff64c0f-f473-41cf-95cd-350dc7acc32a"
              |     }
              | },
              | "timestamp": "2023-05-15 16:23:46.609"
              |}
              |]
      """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-blueprint-guidance-updated-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()

            // Check for 'noOfQuestions'
            fst.getAs[Int]("noOfQuestions") shouldBe 1

            // Check for 'guidanceVariableLessons' (nested structure)
            val guidanceLessons = fst.getAs[Seq[Row]]("guidanceVariableLessons")
            guidanceLessons.size shouldBe 1

            val firstLesson = guidanceLessons.head

            // Check for 'id' in the first lesson
            firstLesson.getAs[String]("id") shouldBe "lesson1"

            // Check for 'classes' inside the first lesson (nested inside guidanceVariableLessons)
            val classes = firstLesson.getAs[Seq[Row]]("classes")
            classes.size shouldBe 2

            // Check individual classes
            val firstClass = classes.head
            firstClass.getAs[String]("id") shouldBe "class1"
            firstClass.getAs[String]("materialType") shouldBe "CORE"
            firstClass.getAs[String]("materialId") shouldBe "material123"

            val secondClass = classes(1)
            secondClass.getAs[String]("id") shouldBe "class2"
            secondClass.getAs[String]("materialType") shouldBe "PATHWAY"
            secondClass.getAs[String]("materialId") shouldBe "material456"

          }
        }
      )
    }
  }

  test("handle TestBlueprintMadeReadyIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "id",
        "contextFrameClassId",
        "contextFrameTitle",
        "contextDomainId",
        "contextFrameDescription",
        "guidanceVariableLessons",
        "noOfQuestions",
        "guidanceType",
        "guidanceVersion",
        "updatedBy",
        "updatedAt",
        "status",
        "occurredOn",
        "aggregateIdentifier",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"TestBlueprintMadeReadyIntegrationEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "id": "eff64c0f-f473-41cf-95cd-350dc7acc32a",
              |	      "contextFrameClassId": "0afcefd7-556f-4b8f-a6c4-f7a2867e5e76",
              |	      "contextFrameTitle": "T2",
              |	      "contextDomainId": "5ba14edb-464e-4cb9-b5e9-0da24713ac45",
              |	      "contextFrameDescription": "Test Context",
              |       "guidanceVariableLessons": [
              |          {
              |            "id": "lesson1",
              |            "classes": [
              |              {
              |                "id": "class1",
              |                "materialType": "CORE",
              |                "materialId": "material123"
              |              }
              |            ]
              |          }
              |       ],
              |       "noOfQuestions": 1,
              |       "guidanceType": "LESSONS_BASED",
              |       "guidanceVersion": {
              |         "major": 1,
              |         "minor": 0,
              |         "revision": 0
              |       },
              |       "updatedBy": "user123",
              |       "updatedAt": "2024-03-12T11:34:18Z",
              |	      "status": "READY",
              |	      "occurredOn": 1710243258020,
              |	      "aggregateIdentifier": "eff64c0f-f473-41cf-95cd-350dc7acc32a"
              |     }
              | },
              | "timestamp": "2023-05-15 16:23:46.609"
              |}
              |]
          """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-blueprint-made-ready-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()

            // Check top-level fields
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "TestBlueprintMadeReadyIntegrationEvent"
            fst.getAs[String]("id") shouldBe "eff64c0f-f473-41cf-95cd-350dc7acc32a"
            fst.getAs[String]("status") shouldBe "READY"
            fst.getAs[String]("occurredOn") shouldBe "2024-03-12 11:34:18.020"
            fst.getAs[String]("aggregateIdentifier") shouldBe "eff64c0f-f473-41cf-95cd-350dc7acc32a"

            // Check nested context fields
            fst.getAs[String]("contextFrameClassId") shouldBe "0afcefd7-556f-4b8f-a6c4-f7a2867e5e76"
            fst.getAs[String]("contextFrameTitle") shouldBe "T2"
            fst.getAs[String]("contextDomainId") shouldBe "5ba14edb-464e-4cb9-b5e9-0da24713ac45"
            fst.getAs[String]("contextFrameDescription") shouldBe "Test Context"

            // Check guidance details
            fst.getAs[Int]("noOfQuestions") shouldBe 1
            fst.getAs[String]("guidanceType") shouldBe "LESSONS_BASED"

            val guidanceVersion = fst.getAs[Row]("guidanceVersion")
            guidanceVersion.getAs[Int]("major") shouldBe 1
            guidanceVersion.getAs[Int]("minor") shouldBe 0
            guidanceVersion.getAs[Int]("revision") shouldBe 0

            // Check for nested guidance lessons
            val guidanceLessons = fst.getAs[Seq[Row]]("guidanceVariableLessons")
            guidanceLessons.size shouldBe 1

            val firstLesson = guidanceLessons.head
            firstLesson.getAs[String]("id") shouldBe "lesson1"

            val classes = firstLesson.getAs[Seq[Row]]("classes")
            classes.size shouldBe 1

            val firstClass = classes.head
            firstClass.getAs[String]("id") shouldBe "class1"
            firstClass.getAs[String]("materialType") shouldBe "CORE"
            firstClass.getAs[String]("materialId") shouldBe "material123"
          }
        }
      )
    }
  }

  test("handle TestBlueprintContextUpdatedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "id",
        "contextFrameClassId",
        "contextFrameTitle",
        "contextDomainId",
        "updatedBy",
        "updatedAt",
        "status",
        "occurredOn",
        "aggregateIdentifier",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"TestBlueprintContextUpdatedIntegrationEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "id": "eff64c0f-f473-41cf-95cd-350dc7acc32a",
              |	      "contextFrameClassId": "0afcefd7-556f-4b8f-a6c4-f7a2867e5e76",
              |	      "contextFrameTitle": "T2",
              |	      "contextDomainId": "5ba14edb-464e-4cb9-b5e9-0da24713ac45",
              |       "updatedBy": "user123",
              |       "updatedAt": "2024-03-12T11:34:18Z",
              |	      "status": "READY",
              |	      "occurredOn": 1710243258020,
              |	      "aggregateIdentifier": "eff64c0f-f473-41cf-95cd-350dc7acc32a"
              |     }
              | },
              | "timestamp": "2023-05-15 16:23:46.609"
              |}
              |]
          """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-blueprint-context-updated-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()

            // Check top-level fields
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "TestBlueprintContextUpdatedIntegrationEvent"
            fst.getAs[String]("id") shouldBe "eff64c0f-f473-41cf-95cd-350dc7acc32a"
            fst.getAs[String]("status") shouldBe "READY"
            fst.getAs[String]("occurredOn") shouldBe "2024-03-12 11:34:18.020"
            fst.getAs[String]("aggregateIdentifier") shouldBe "eff64c0f-f473-41cf-95cd-350dc7acc32a"

            // Check context fields
            fst.getAs[String]("contextFrameClassId") shouldBe "0afcefd7-556f-4b8f-a6c4-f7a2867e5e76"
            fst.getAs[String]("contextFrameTitle") shouldBe "T2"
            fst.getAs[String]("contextDomainId") shouldBe "5ba14edb-464e-4cb9-b5e9-0da24713ac45"

            // Check for updatedBy and updatedAt (null in this case)
            fst.getAs[String]("updatedBy") shouldBe "user123"
            fst.getAs[String]("updatedAt") shouldBe "2024-03-12T11:34:18Z"
          }
        }
      )
    }
  }

  test("handle TestBlueprintPublishedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "updatedAt",
        "eventDateDw",
        "aggregateIdentifier",
        "loadtime",
        "occurredOn",
        "id",
        "status",
        "publishedBy",
        "tenantId",
        "updatedBy",
        "createdBy",
        "createdAt",
        "publishedAt",
        "contextFrameClassId",
        "contextFrameTitle",
        "contextDomainId",
        "noOfQuestions",
        "guidanceType",
        "guidanceVersion",
        "guidanceVariableLessons"
      )

      // Sample guidance lessons in JSON format
      val guidanceLessonsJson = """
                                  |[
                                  |  {
                                  |    "id": "lesson1",
                                  |    "classes": [
                                  |      {"id": "class1", "materialType": "CORE", "materialId": "material1"},
                                  |      {"id": "class2", "materialType": "PATHWAY", "materialId": "material2"}
                                  |    ]
                                  |  },
                                  |  {
                                  |    "id": "lesson2",
                                  |    "classes": [
                                  |      {"id": "class3", "materialType": "CORE", "materialId": "material3"}
                                  |    ]
                                  |  }
                                  |]
    """.stripMargin

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = s"""
                     |[
                     |{
                     | "key": "key1",
                     | "value":{
                     |    "headers":{
                     |       "eventType":"TestBlueprintPublishedIntegrationEvent",
                     |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                     |    },
                     |    "body":{
                     |       "id": "eff64c0f-f473-41cf-95cd-350dc7acc32a",
                     |       "contextFrameClassId": "frameClassId123",
                     |       "contextFrameTitle": "Frame Title Example",
                     |       "contextDomainId": "domainId123",
                     |       "guidanceVariableLessons": $guidanceLessonsJson,
                     |       "noOfQuestions": 10,
                     |       "guidanceType": "exampleGuidanceType",
                     |       "guidanceVersion": {"major": 1, "minor": 0, "revision": 0},
                     |       "updatedBy": "user123",
                     |       "updatedAt": "2024-03-12T11:34:18Z",
                     |       "createdBy": "user123",
                     |       "createdAt": "2024-03-12T11:34:18Z",
                     |       "publishedBy": "publisher123",
                     |       "publishedAt": "2024-03-12T11:34:18Z",
                     |       "status": "READY",
                     |       "occurredOn": 1710243258020,
                     |       "aggregateIdentifier": "eff64c0f-f473-41cf-95cd-350dc7acc32a"
                     |     }
                     | },
                     | "timestamp": "2023-05-15 16:23:46.609"
                     |}
                     |]
          """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-blueprint-published-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "TestBlueprintPublishedIntegrationEvent"
            fst.getAs[String]("id") shouldBe "eff64c0f-f473-41cf-95cd-350dc7acc32a"
            fst.getAs[String]("contextFrameClassId") shouldBe "frameClassId123"
            fst.getAs[String]("contextFrameTitle") shouldBe "Frame Title Example"
            fst.getAs[String]("contextDomainId") shouldBe "domainId123"
            fst.getAs[Int]("noOfQuestions") shouldBe 10
            fst.getAs[String]("guidanceType") shouldBe "exampleGuidanceType"
            val guidanceVersion = fst.getAs[Row]("guidanceVersion")
            guidanceVersion.getAs[Int]("major") shouldBe 1
            guidanceVersion.getAs[Int]("minor") shouldBe 0
            guidanceVersion.getAs[Int]("revision") shouldBe 0

            fst.getAs[String]("updatedBy") shouldBe "user123"
            fst.getAs[String]("updatedAt") shouldBe "2024-03-12T11:34:18Z"
            fst.getAs[String]("createdBy") shouldBe "user123"
            fst.getAs[String]("createdAt") shouldBe "2024-03-12T11:34:18Z"
            fst.getAs[String]("publishedBy") shouldBe "publisher123"
            fst.getAs[String]("publishedAt") shouldBe "2024-03-12T11:34:18Z"
            fst.getAs[String]("status") shouldBe "READY"
            fst.getAs[String]("occurredOn") shouldBe "2024-03-12 11:34:18.020"
            fst.getAs[String]("aggregateIdentifier") shouldBe "eff64c0f-f473-41cf-95cd-350dc7acc32a"

            // Validate guidanceVariableLessons
            val guidanceLessons = fst.getAs[Seq[Row]]("guidanceVariableLessons")
            guidanceLessons.size shouldBe 2 // Expecting 2 lessons in the JSON

            // Check first guidance lesson
            val firstLesson = guidanceLessons.head
            firstLesson.getAs[String]("id") shouldBe "lesson1"

            val classes = firstLesson.getAs[Seq[Row]]("classes")
            classes.size shouldBe 2 // Expecting 2 classes in the first lesson

            // Check first class in the first lesson
            val firstClass = classes.head
            firstClass.getAs[String]("id") shouldBe "class1"
            firstClass.getAs[String]("materialType") shouldBe "CORE"
            firstClass.getAs[String]("materialId") shouldBe "material1"

            // Check second class in the first lesson
            val secondClass = classes(1)
            secondClass.getAs[String]("id") shouldBe "class2"
            secondClass.getAs[String]("materialType") shouldBe "PATHWAY"
            secondClass.getAs[String]("materialId") shouldBe "material2"

            // Check second guidance lesson
            val secondLesson = guidanceLessons(1)
            secondLesson.getAs[String]("id") shouldBe "lesson2"

            val secondClasses = secondLesson.getAs[Seq[Row]]("classes")
            secondClasses.size shouldBe 1 // Expecting 1 class in the second lesson

            // Check class in the second lesson
            val secondClassInLesson2 = secondClasses.head
            secondClassInLesson2.getAs[String]("id") shouldBe "class3"
            secondClassInLesson2.getAs[String]("materialType") shouldBe "CORE"
            secondClassInLesson2.getAs[String]("materialId") shouldBe "material3"
          }
        }
      )
    }
  }

  test("handle TestCreatedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "contextFrameTitle",
        "aggregateIdentifier",
        "contextDomainId",
        "loadtime",
        "contextFrameClassId",
        "items",
        "occurredOn",
        "status",
        "testId",
        "createdAt",
        "createdBy",
        "testBlueprintId",
        "tenantId",
        "contextFrameDescription"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"TestCreatedIntegrationEvent",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |       "testId": "test1",
                    |       "testBlueprintId": "blueprint1",
                    |       "items": [
                    |         {
                    |           "id": "item1",
                    |           "type": "multiple_choice"
                    |         }
                    |       ],
                    |       "contextFrameClassId": "class1",
                    |       "contextFrameTitle": "Math Test",
                    |       "contextFrameDescription": "Test for basic math skills",
                    |       "contextDomainId": "domain1",
                    |       "status": "CREATED",
                    |       "createdBy": "user1",
                    |       "createdAt": "2024-03-12T11:34:18.020Z",
                    |       "occurredOn": 1710243258020,
                    |       "aggregateIdentifier": "agg1"
                    |     }
                    | },
                    | "timestamp": "2023-05-15 16:23:46.609"
                    |}
                    |]
          """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-created-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("testId") shouldBe "test1"
            fst.getAs[String]("testBlueprintId") shouldBe "blueprint1"
            fst.getAs[String]("contextFrameClassId") shouldBe "class1"
            fst.getAs[String]("contextFrameTitle") shouldBe "Math Test"
            fst.getAs[String]("contextFrameDescription") shouldBe "Test for basic math skills"
            fst.getAs[String]("contextDomainId") shouldBe "domain1"
            fst.getAs[String]("status") shouldBe "CREATED"
            fst.getAs[String]("createdBy") shouldBe "user1"
            fst.getAs[String]("aggregateIdentifier") shouldBe "agg1"
            fst.getAs[String]("occurredOn") shouldBe "2024-03-12 11:34:18.020"

            // Check for nested items
            val items = fst.getAs[Seq[Row]]("items")
            items.size shouldBe 1

            val firstItem = items.head
            firstItem.getAs[String]("id") shouldBe "item1"
            firstItem.getAs[String]("type") shouldBe "multiple_choice"
          }
        }
      )
    }
  }

  test("handle TestContextUpdatedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "updatedAt",
        "eventDateDw",
        "aggregateIdentifier",
        "contextDomainId",
        "loadtime",
        "contextFrameAreItemsShuffled",
        "occurredOn",
        "testId",
        "tenantId",
        "updatedBy"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"TestContextUpdatedIntegrationEvent",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |       "testId": "1441eddb-35ac-421d-a85a-4eaad2cebefe",
                    |       "updatedBy": "b484d15f-1559-4fa1-9f9f-f6fe139943c4",
                    |       "updatedAt": "2024-03-25T08:17:22.293549827",
                    |       "contextDomainId": "domainId-5678",
                    |       "contextFrameAreItemsShuffled": false,
                    |       "occurredOn": 1711354642000,
                    |       "aggregateIdentifier": "1441eddb-35ac-421d-a85a-4eaad2cebefe"
                    |     }
                    | },
                    | "timestamp": "2023-05-15 16:23:46.609"
                    |}
                    |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-context-updated-sink").map(_.input)
          val df = dfOpt.get

          assert(df.columns.toSet == expectedColumns)
          assert(df.collect().length == 1)

          val fst = df.first()
          assert(fst.getAs[String]("tenantId") == "93e4949d-7eff-4707-9201-dac917a5e013")
          assert(fst.getAs[String]("eventType") == "TestContextUpdatedIntegrationEvent")
          assert(fst.getAs[String]("testId") == "1441eddb-35ac-421d-a85a-4eaad2cebefe")
          assert(fst.getAs[String]("updatedBy") == "b484d15f-1559-4fa1-9f9f-f6fe139943c4")
          assert(fst.getAs[String]("updatedAt") == "2024-03-25T08:17:22.293549827")
          assert(fst.getAs[String]("contextDomainId") == "domainId-5678")
          assert(!fst.getAs[Boolean]("contextFrameAreItemsShuffled"))

          // Convert occurredOn from milliseconds to the appropriate timestamp format
          assert(fst.getAs[String]("occurredOn") == "2024-03-25 08:17:22.000")

          assert(fst.getAs[String]("aggregateIdentifier") == "1441eddb-35ac-421d-a85a-4eaad2cebefe")
        }
      )
    }
  }

  test("handle ItemReorderedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "testId",
        "updatedBy",
        "updatedAt",
        "occurredOn",
        "aggregateIdentifier",
        "tenantId",
        "eventDateDw",
        "loadtime",
        "items"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"ItemReorderedIntegrationEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "testId": "531f897f-4999-4902-ac2e-3c2175855f0a",
              |       "items": [
              |          {
              |             "id": "3011b10d-dd16-429f-8ffc-3f0e4ec95e8b",
              |             "type": "QUESTION"
              |          },
              |          {
              |             "id": "ca77feb5-5749-41c3-8308-f5dc3f4c9cac",
              |             "type": "QUESTION"
              |          },
              |          {
              |             "id": "0e181a8e-9020-4211-a28b-beb653cd7bfb",
              |             "type": "QUESTION"
              |          },
              |          {
              |             "id": "98731e66-d317-4983-8141-4993c3f5f9de",
              |             "type": "QUESTION"
              |          }
              |       ],
              |       "updatedBy": "96049cee-81eb-4fb4-95c3-99f625dae07c",
              |       "updatedAt": "2024-03-26T06:03:06.437810185",
              |       "occurredOn": 1711432986,
              |       "aggregateIdentifier": "531f897f-4999-4902-ac2e-3c2175855f0a"
              |     }
              | },
              | "timestamp": "2023-05-15 16:23:46.609"
              |}
              |]
              """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-item-reordered-sink").map(_.input)
          assert(dfOpt.isDefined)
          val df = dfOpt.get

          assert(df.columns.toSet == expectedColumns)
          assert(df.collect().length == 1)

          val fst = df.first()
          assert(fst.getAs[String]("tenantId") == "93e4949d-7eff-4707-9201-dac917a5e013")
          assert(fst.getAs[String]("eventType") == "ItemReorderedIntegrationEvent")
          assert(fst.getAs[String]("testId") == "531f897f-4999-4902-ac2e-3c2175855f0a")
          assert(fst.getAs[String]("updatedBy") == "96049cee-81eb-4fb4-95c3-99f625dae07c")
          assert(fst.getAs[String]("updatedAt") == "2024-03-26T06:03:06.437810185")
          assert(fst.getAs[String]("occurredOn") == "1970-01-20 19:23:52.986")
          assert(fst.getAs[String]("aggregateIdentifier") == "531f897f-4999-4902-ac2e-3c2175855f0a")
        }
      )
    }
  }

  test("handle ItemReplacedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set("eventType",
                                             "updatedAt",
                                             "eventDateDw",
                                             "loadtime",
                                             "items",
                                             "occurredOn",
                                             "testId",
                                             "tenantId",
                                             "updatedBy",
                                             "aggregateIdentifier")

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"ItemReplacedIntegrationEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "testId": "3c16d9fa-275c-4747-88c6-c477c4c31733",
              |       "items": [
              |          {
              |             "id": "c04c0803-565c-44b7-b9b4-c3bb09a0da30",
              |             "type": "QUESTION"
              |          }
              |       ],
              |       "updatedBy": "58b0934e-1a8b-48b9-bba2-dfc0c474e117",
              |       "updatedAt": "2024-03-26T10:58:10.390181415",
              |       "occurredOn": 1711450690,
              |       "aggregateIdentifier": "3c16d9fa-275c-4747-88c6-c477c4c31733"
              |     }
              | },
              | "timestamp": "2023-05-15 16:23:46.609"
              |}
              |]
              """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-item-replaced-sink").map(_.input)
          assert(dfOpt.isDefined)
          val df = dfOpt.get

          assert(df.columns.toSet == expectedColumns)
          assert(df.collect().length == 1)

          val fst = df.first()
          assert(fst.getAs[String]("tenantId") == "93e4949d-7eff-4707-9201-dac917a5e013")
          assert(fst.getAs[String]("eventType") == "ItemReplacedIntegrationEvent")
          assert(fst.getAs[String]("testId") == "3c16d9fa-275c-4747-88c6-c477c4c31733")
          assert(fst.getAs[String]("updatedBy") == "58b0934e-1a8b-48b9-bba2-dfc0c474e117")
          assert(fst.getAs[String]("updatedAt") == "2024-03-26T10:58:10.390181415")
          assert(fst.getAs[String]("occurredOn") == "1970-01-20 19:24:10.690")
          assert(fst.getAs[String]("aggregateIdentifier") == "3c16d9fa-275c-4747-88c6-c477c4c31733")
        }
      )
    }
  }

  test("handle TestItemsRegeneratedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] =
        Set("eventType", "updatedAt", "eventDateDw", "loadtime", "items", "occurredOn", "testId", "tenantId", "aggregateIdentifier")

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"TestItemsRegeneratedIntegrationEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "testId": "3c16d9fa-275c-4747-88c6-c477c4c31733",
              |       "items": [
              |          {
              |             "id": "bb2d1fd9-d76d-4982-a7b6-bddf904f2795",
              |             "type": "QUESTION"
              |          }
              |       ],
              |       "updatedAt": "2024-03-26T11:16:02.936926457",
              |       "occurredOn": 1711451762,
              |       "aggregateIdentifier": "3c16d9fa-275c-4747-88c6-c477c4c31733"
              |     }
              | },
              | "timestamp": "2023-05-15 16:23:46.609"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-items-regenerated-sink").map(_.input)
          assert(dfOpt.isDefined)
          val df = dfOpt.get

          assert(df.columns.toSet == expectedColumns)
          assert(df.collect().length == 1)

          val fst = df.first()
          assert(fst.getAs[String]("tenantId") == "93e4949d-7eff-4707-9201-dac917a5e013")
          assert(fst.getAs[String]("eventType") == "TestItemsRegeneratedIntegrationEvent")
          assert(fst.getAs[String]("testId") == "3c16d9fa-275c-4747-88c6-c477c4c31733")
          assert(fst.getAs[String]("updatedAt") == "2024-03-26T11:16:02.936926457")
          assert(fst.getAs[String]("occurredOn") == "1970-01-20 19:24:11.762")
          assert(fst.getAs[String]("aggregateIdentifier") == "3c16d9fa-275c-4747-88c6-c477c4c31733")
        }
      )
    }
  }

  test("handle TestPublishedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "testId",
        "testBlueprintId",
        "items",
        "contextFrameClassId",
        "contextFrameTitle",
        "contextFrameDescription",
        "contextDomainId",
        "updatedBy",
        "updatedAt",
        "publishedBy",
        "publishedAt",
        "status",
        "occurredOn",
        "aggregateIdentifier",
        "tenantId",
        "loadtime",
        "eventDateDw"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"TestPublishedIntegrationEvent",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |       "testId": "3c16d9fa-275c-4747-88c6-c477c4c31733",
                    |       "testBlueprintId": "blueprint-123",
                    |       "items": [
                    |         {"id": "item1", "type": "multiple-choice"},
                    |         {"id": "item2", "type": "true-false"}
                    |       ],
                    |       "contextFrameClassId": "context-123",
                    |       "contextFrameTitle": "Test Title",
                    |       "contextFrameDescription": "Test Description",
                    |       "contextDomainId": "domain-xyz",
                    |       "updatedBy": "b484d15f-1559-4fa1-9f9f-f6fe139943c4",
                    |       "updatedAt": "2024-03-26T11:16:02.936926457",
                    |       "publishedBy": "publish_user_id",
                    |       "publishedAt": "2024-03-26T11:16:02.936926457",
                    |       "status": "PUBLISHED",
                    |       "occurredOn": 1711451762000,
                    |       "aggregateIdentifier": "3c16d9fa-275c-4747-88c6-c477c4c31733"
                    |     }
                    | },
                    | "timestamp": "2023-05-15 16:23:46.609"
                    |}
                    |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-published-sink").map(_.input)
          assert(dfOpt.isDefined)
          val df = dfOpt.get

          assert(df.columns.toSet == expectedColumns)
          assert(df.collect().length == 1)

          val fst = df.first()
          assert(fst.getAs[String]("tenantId") == "93e4949d-7eff-4707-9201-dac917a5e013")
          assert(fst.getAs[String]("eventType") == "TestPublishedIntegrationEvent")
          assert(fst.getAs[String]("testId") == "3c16d9fa-275c-4747-88c6-c477c4c31733")
          assert(fst.getAs[String]("testBlueprintId") == "blueprint-123")
          assert(fst.getAs[String]("contextFrameClassId") == "context-123")
            assert(fst.getAs[String]("contextFrameTitle") == "Test Title")
            assert(fst.getAs[String]("contextFrameDescription") == "Test Description")
          assert(fst.getAs[String]("contextDomainId") == "domain-xyz")
          assert(fst.getAs[String]("updatedBy") == "b484d15f-1559-4fa1-9f9f-f6fe139943c4")
          assert(fst.getAs[String]("updatedAt") == "2024-03-26T11:16:02.936926457")
          assert(fst.getAs[String]("publishedBy") == "publish_user_id")
          assert(fst.getAs[String]("publishedAt") == "2024-03-26T11:16:02.936926457")
          assert(fst.getAs[String]("status") == "PUBLISHED")
          assert(fst.getAs[String]("occurredOn") == "2024-03-26 11:16:02.000")
          assert(fst.getAs[String]("aggregateIdentifier") == "3c16d9fa-275c-4747-88c6-c477c4c31733")

          // Check for nested items
          val items = fst.getAs[Seq[Row]]("items")
          items.size shouldBe 2

          val firstItem = items.head
          assert(firstItem.getAs[String]("id") == "item1")
          assert(firstItem.getAs[String]("type") == "multiple-choice")

          val secondItem = items(1)
          assert(secondItem.getAs[String]("id") == "item2")
          assert(secondItem.getAs[String]("type") == "true-false")
        }
      )
    }
  }

  test("handle TestDeliveryCreatedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "aggregateIdentifier",
        "loadtime",
        "contextFrameClassId",
        "contextDomainId",
        "candidates",
        "deliverySettings",
        "occurredOn",
        "id",
        "status",
        "testId",
        "allPossibleCandidates",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"TestDeliveryCreatedIntegrationEvent",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |       "id": "40ea6fab-fca5-40c0-8065-23b731aff9ed",
                    |       "testId": "80bc17d5-95c3-442b-af21-5b69fc8d5a93",
                    |       "contextFrameClassId": "58e158bf-020f-45a5-bd94-732cb5c2a91c",
                    |       "contextDomainId": "5ba14edb-464e-4cb9-b5e9-0da24713ac45",
                    |       "candidates": [
                    |          "1665b58b-9eb4-42a1-92b3-e0588717fbf1",
                    |          "17e4ab6b-da94-4970-81a4-bdfc2e5cae27"
                    |       ],
                    |       "deliverySettings": {
                    |          "title": "cc",
                    |          "startTime": "2024-01-30T08:03:22.985",
                    |          "endTime": null,
                    |          "allowLateSubmission": true,
                    |          "stars": 5,
                    |          "randomized": false
                    |       },
                    |       "status": "UPCOMING",
                    |       "allPossibleCandidates": [
                    |          "e3c2968c-8481-41de-a6b3-d68652c3e922",
                    |          "6cbfbd95-a113-4fdb-87ab-636ebc160d54"
                    |       ],
                    |       "occurredOn": 1706601833476,
                    |       "aggregateIdentifier": "40ea6fab-fca5-40c0-8065-23b731aff9ed"
                    |     }
                    | },
                    | "timestamp": "2023-05-15 16:23:46.609"
                    |}
                    |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-delivery-created-sink").map(_.input)
          assert(dfOpt.isDefined)
          val df = dfOpt.get

          assert(df.columns.toSet == expectedColumns)
          assert(df.collect().length == 1)

          val fst = df.first()
          assert(fst.getAs[String]("tenantId") == "93e4949d-7eff-4707-9201-dac917a5e013")
          assert(fst.getAs[String]("eventType") == "TestDeliveryCreatedIntegrationEvent")
          assert(fst.getAs[String]("id") == "40ea6fab-fca5-40c0-8065-23b731aff9ed")
          assert(fst.getAs[String]("testId") == "80bc17d5-95c3-442b-af21-5b69fc8d5a93")
          assert(fst.getAs[String]("contextFrameClassId") == "58e158bf-020f-45a5-bd94-732cb5c2a91c")
          assert(fst.getAs[String]("contextDomainId") == "5ba14edb-464e-4cb9-b5e9-0da24713ac45")
          assert(fst.getAs[String]("status") == "UPCOMING")

          // Check candidates
          assert(fst.getAs[Seq[String]]("candidates") == Seq("1665b58b-9eb4-42a1-92b3-e0588717fbf1", "17e4ab6b-da94-4970-81a4-bdfc2e5cae27"))

          // Check allPossibleCandidates
          assert(fst.getAs[Seq[String]]("allPossibleCandidates") == Seq("e3c2968c-8481-41de-a6b3-d68652c3e922", "6cbfbd95-a113-4fdb-87ab-636ebc160d54"))

          // Check deliverySettings
          val deliverySettings = fst.getAs[Row]("deliverySettings")
          assert(deliverySettings.getAs[String]("title") == "cc")
          assert(deliverySettings.getAs[String]("startTime") == "2024-01-30T08:03:22.985")
          assert(deliverySettings.getAs[Boolean]("allowLateSubmission"))
          assert(deliverySettings.getAs[Int]("stars") == 5)
          assert(!deliverySettings.getAs[Boolean]("randomized"))

          assert(fst.getAs[String]("occurredOn") == "2024-01-30 08:03:53.476")
          assert(fst.getAs[String]("aggregateIdentifier") == "40ea6fab-fca5-40c0-8065-23b731aff9ed")
        }
      )
    }
  }

  test("handle TestDeliveryArchivedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "aggregateIdentifier",
        "loadtime",
        "updatedAt",
        "updatedBy",
        "id",
        "status",
        "tenantId",
        "occurredOn"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"TestDeliveryArchivedIntegrationEvent",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |       "id": "a45582e1-dc13-42d1-8030-91360a00c9d7",
                    |       "status": "DISCARDED",
                    |       "updatedBy": "e3c2968c-8481-41de-a6b3-d68652c3e922",
                    |       "updatedAt": "2025-01-08T20:37:33.738014",
                    |       "occurredOn": 1706601833476,
                    |       "aggregateIdentifier": "40ea6fab-fca5-40c0-8065-23b731aff9ed"
                    |     }
                    | },
                    | "timestamp": "2025-01-08 16:23:46.609"
                    |}
                    |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-delivery-archived-sink").map(_.input)
          assert(dfOpt.isDefined)
          val df = dfOpt.get

          assert(df.columns.toSet == expectedColumns)
          assert(df.collect().length == 1)

          val fst = df.first()
          assert(fst.getAs[String]("tenantId") == "93e4949d-7eff-4707-9201-dac917a5e013")
          assert(fst.getAs[String]("eventType") == "TestDeliveryArchivedIntegrationEvent")
          assert(fst.getAs[String]("id") == "a45582e1-dc13-42d1-8030-91360a00c9d7")
          assert(fst.getAs[String]("status") == "DISCARDED")
        }
      )
    }
  }

  test("handle TestDeliveryStartedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "aggregateIdentifier",
        "loadtime",
        "contextFrameClassId",
        "contextDomainId",
        "candidates",
        "deliverySettings",
        "occurredOn",
        "id",
        "status",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"TestDeliveryStartedIntegrationEvent",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |       "id": "e904d2c3-506e-4997-90e1-56836eadbd97",
                    |       "contextFrameClassId": "4d7bdd2a-8514-4f92-ba35-0033a0adc2e4",
                    |       "contextDomainId": "5ba14edb-464e-4cb9-b5e9-0da24713ac45",
                    |       "candidates": [
                    |          "fbd90506-cebe-4135-a1f9-d01f3479e260",
                    |          "b03d7e71-5df9-4d67-a47f-2d8f948dd093"
                    |       ],
                    |       "deliverySettings": {
                    |          "title": "new test ",
                    |          "startTime": "2024-02-01T12:32:36.896",
                    |          "endTime": "2024-02-01T19:59:59.999",
                    |          "allowLateSubmission": true,
                    |          "stars": 3,
                    |          "randomized": false
                    |       },
                    |       "status": "ONGOING",
                    |       "occurredOn": 1706790780276,
                    |       "aggregateIdentifier": "e904d2c3-506e-4997-90e1-56836eadbd97"
                    |     }
                    | },
                    | "timestamp": "2023-05-15 16:23:46.609"
                    |}
                    |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-delivery-started-sink").map(_.input)
          assert(dfOpt.isDefined)
          val df = dfOpt.get

          assert(df.columns.toSet == expectedColumns)
          assert(df.collect().length == 1)

          val fst = df.first()
          assert(fst.getAs[String]("tenantId") == "93e4949d-7eff-4707-9201-dac917a5e013")
          assert(fst.getAs[String]("eventType") == "TestDeliveryStartedIntegrationEvent")
          assert(fst.getAs[String]("id") == "e904d2c3-506e-4997-90e1-56836eadbd97")
          assert(fst.getAs[String]("contextFrameClassId") == "4d7bdd2a-8514-4f92-ba35-0033a0adc2e4")
          assert(fst.getAs[String]("contextDomainId") == "5ba14edb-464e-4cb9-b5e9-0da24713ac45")

          // Check candidates
          assert(fst.getAs[Seq[String]]("candidates") == Seq("fbd90506-cebe-4135-a1f9-d01f3479e260", "b03d7e71-5df9-4d67-a47f-2d8f948dd093"))

          // Check deliverySettings
          val deliverySettings = fst.getAs[Row]("deliverySettings")
          assert(deliverySettings.getAs[String]("title") == "new test ")
          assert(deliverySettings.getAs[String]("startTime") == "2024-02-01T12:32:36.896")
          assert(deliverySettings.getAs[String]("endTime") == "2024-02-01T19:59:59.999")
          assert(deliverySettings.getAs[Boolean]("allowLateSubmission"))
          assert(deliverySettings.getAs[Int]("stars") == 3)
          assert(!deliverySettings.getAs[Boolean]("randomized"))

          assert(fst.getAs[String]("status") == "ONGOING")
          assert(fst.getAs[String]("occurredOn") == "2024-02-01 12:33:00.276")
          assert(fst.getAs[String]("aggregateIdentifier") == "e904d2c3-506e-4997-90e1-56836eadbd97")
        }
      )
    }
  }

  test("handle TestDeliveryCandidateUpdatedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "aggregateIdentifier",
        "loadtime",
        "candidates",
        "testId",
        "contextFrameClassId",
        "deliverySettings",
        "occurredOn",
        "id",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"TestDeliveryCandidateUpdatedIntegrationEvent",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |       "id": "90e6bd7c-8b1f-4b16-805d-0ac98d323ade",
                    |       "candidates": [
                    |          "c4da5961-fdf6-4127-bc12-1377032ac443",
                    |          "d6dd7f8e-25a5-4f0c-8aab-bc4b796180ca"
                    |       ],
                    |       "testId": "7e69a5d3-48c2-4142-b13d-8e72de467c59",
                    |       "contextFrameClassId": "5fba2c0a-2434-474d-a65c-93438e4b7f13",
                    |       "deliverySettings": {
                    |          "title": "Updated Test Delivery",
                    |          "startTime": "2024-02-08T10:00:00.000",
                    |          "endTime": "2024-02-08T12:00:00.000",
                    |          "allowLateSubmission": false,
                    |          "stars": 4,
                    |          "randomized": true
                    |       },
                    |       "occurredOn": 1707376397076,
                    |       "aggregateIdentifier": "90e6bd7c-8b1f-4b16-805d-0ac98d323ade"
                    |     }
                    | },
                    | "timestamp": "2023-05-15 16:23:46.609"
                    |}
                    |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-delivery-candidate-updated-sink").map(_.input)
          assert(dfOpt.isDefined)
          val df = dfOpt.get

          assert(df.columns.toSet == expectedColumns)
          assert(df.collect().length == 1)

          val fst = df.first()
          assert(fst.getAs[String]("tenantId") == "93e4949d-7eff-4707-9201-dac917a5e013")
          assert(fst.getAs[String]("eventType") == "TestDeliveryCandidateUpdatedIntegrationEvent")
          assert(fst.getAs[String]("id") == "90e6bd7c-8b1f-4b16-805d-0ac98d323ade")
          assert(fst.getAs[Seq[String]]("candidates") == Seq("c4da5961-fdf6-4127-bc12-1377032ac443", "d6dd7f8e-25a5-4f0c-8aab-bc4b796180ca"))
          assert(fst.getAs[String]("testId") == "7e69a5d3-48c2-4142-b13d-8e72de467c59")
          assert(fst.getAs[String]("contextFrameClassId") == "5fba2c0a-2434-474d-a65c-93438e4b7f13")

          // Check deliverySettings
          val deliverySettings = fst.getAs[Row]("deliverySettings")
          assert(deliverySettings.getAs[String]("title") == "Updated Test Delivery")
          assert(deliverySettings.getAs[String]("startTime") == "2024-02-08T10:00:00.000")
          assert(deliverySettings.getAs[String]("endTime") == "2024-02-08T12:00:00.000")
          assert(!deliverySettings.getAs[Boolean]("allowLateSubmission"))
          assert(deliverySettings.getAs[Int]("stars") == 4)
          assert(deliverySettings.getAs[Boolean]("randomized"))

          assert(fst.getAs[String]("occurredOn") == "2024-02-08 07:13:17.076")
          assert(fst.getAs[String]("aggregateIdentifier") == "90e6bd7c-8b1f-4b16-805d-0ac98d323ade")
        }
      )
    }
  }

  test("handle CandidateSessionRecorderMadeInProgressIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "aggregateIdentifier",
        "loadtime",
        "id",
        "candidateId",
        "deliveryId",
        "assessmentId",
        "createdAt",
        "status",
        "occurredOn",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"CandidateSessionRecorderMadeInProgressIntegrationEvent",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |       "id": "f3490bfc-7ab5-4623-b582-89a69dfe2a9c",
                    |       "candidateId": "6bbdf826-f97d-44f4-8f79-682bb4798e25",
                    |       "deliveryId": "0c3d6782-202b-482d-b8c7-ff9c5fe70b17",
                    |       "assessmentId": "a79e9bc4-c5c3-4a4e-bd9f-c536d5d4c7d6",
                    |       "createdAt": "2024-02-10T15:30:00.000",
                    |       "status": "IN_PROGRESS",
                    |       "occurredOn": 1707552600000,
                    |       "aggregateIdentifier": "f3490bfc-7ab5-4623-b582-89a69dfe2a9c"
                    |     }
                    | },
                    | "timestamp": "2023-05-15 16:23:46.609"
                    |}
                    |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-candidate-session-recorder-made-in-progress-sink").map(_.input)
          assert(dfOpt.isDefined)
          val df = dfOpt.get

          assert(df.columns.toSet == expectedColumns)
          assert(df.collect().length == 1)

          val fst = df.first()
          assert(fst.getAs[String]("tenantId") == "93e4949d-7eff-4707-9201-dac917a5e013")
          assert(fst.getAs[String]("eventType") == "CandidateSessionRecorderMadeInProgressIntegrationEvent")
          assert(fst.getAs[String]("id") == "f3490bfc-7ab5-4623-b582-89a69dfe2a9c")
          assert(fst.getAs[String]("candidateId") == "6bbdf826-f97d-44f4-8f79-682bb4798e25")
          assert(fst.getAs[String]("deliveryId") == "0c3d6782-202b-482d-b8c7-ff9c5fe70b17")
          assert(fst.getAs[String]("assessmentId") == "a79e9bc4-c5c3-4a4e-bd9f-c536d5d4c7d6")
          assert(fst.getAs[String]("createdAt") == "2024-02-10T15:30:00.000")
          assert(fst.getAs[String]("status") == "IN_PROGRESS")
          assert(fst.getAs[String]("occurredOn") == "2024-02-10 08:10:00.000")
          assert(fst.getAs[String]("aggregateIdentifier") == "f3490bfc-7ab5-4623-b582-89a69dfe2a9c")
        }
      )
    }
  }

  test("handle CandidateSessionRecorderMadeCompletedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "aggregateIdentifier",
        "loadtime",
        "id",
        "candidateId",
        "deliveryId",
        "assessmentId",
        "score",
        "awardedStars",
        "createdAt",
        "updatedAt",
        "status",
        "occurredOn",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"CandidateSessionRecorderMadeCompletedIntegrationEvent",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |       "id": "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb",
                    |       "candidateId": "b87d65a8-bc17-41f4-b1a4-cd38c1f10be6",
                    |       "deliveryId": "3278a6e9-0b0b-437e-baae-024fa178b3b8",
                    |       "assessmentId": "8d829b85-54f4-4d82-8301-8fa3f9ad10f2",
                    |       "items": [
                    |          {
                    |             "questionId": "q1",
                    |             "questionCode": "QC001",
                    |             "submissions": [
                    |                {
                    |                   "attemptNumber": 1,
                    |                   "questionVersion": 3,
                    |                   "answer": {
                    |                      "type": "MCQ",
                    |                      "choiceIds": [1, 2],
                    |                      "answerItems": [],
                    |                      "selectedBlankChoices": []
                    |                   },
                    |                   "result": {
                    |                      "correct": true,
                    |                      "score": 2.5,
                    |                      "validResponse": {
                    |                         "type": "MCQ",
                    |                         "choiceIds": [1],
                    |                         "answerMapping": [],
                    |                         "validBlankChoices": []
                    |                      },
                    |                      "answerResponse": {
                    |                         "correctAnswers": {
                    |                            "type": "MCQ",
                    |                            "choiceIds": [1],
                    |                            "answerItems": [],
                    |                            "selectedBlankChoices": []
                    |                         },
                    |                         "wrongAnswers": [],
                    |                         "unattendedCorrectAnswers": []
                    |                      }
                    |                   },
                    |                   "timeSpent": 45.5,
                    |                   "hintUsed": false,
                    |                   "timestamp": "2024-02-12T12:45:23.123"
                    |                }
                    |             ],
                    |             "createdAt": "2024-02-10T09:00:00.000",
                    |             "updatedAt": "2024-02-10T09:30:00.000"
                    |          }
                    |       ],
                    |       "score": 8.5,
                    |       "awardedStars": 4,
                    |       "createdAt": "2024-02-10T09:00:00.000",
                    |       "updatedAt": "2024-02-10T09:30:00.000",
                    |       "status": "COMPLETED",
                    |       "occurredOn": 1707657000000,
                    |       "aggregateIdentifier": "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb"
                    |     }
                    | },
                    | "timestamp": "2023-05-15 16:23:46.609"
                    |}
                    |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-candidate-session-recorder-made-in-completed-sink").map(_.input)
          assert(dfOpt.isDefined)
          val df = dfOpt.get

          assert(df.columns.toSet == expectedColumns)
          assert(df.collect().length == 1)

          val fst = df.first()
          assert(fst.getAs[String]("tenantId") == "93e4949d-7eff-4707-9201-dac917a5e013")
          assert(fst.getAs[String]("eventType") == "CandidateSessionRecorderMadeCompletedIntegrationEvent")
          assert(fst.getAs[String]("id") == "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb")
          assert(fst.getAs[String]("candidateId") == "b87d65a8-bc17-41f4-b1a4-cd38c1f10be6")
          assert(fst.getAs[String]("deliveryId") == "3278a6e9-0b0b-437e-baae-024fa178b3b8")
          assert(fst.getAs[String]("assessmentId") == "8d829b85-54f4-4d82-8301-8fa3f9ad10f2")
          assert(fst.getAs[String]("createdAt") == "2024-02-10T09:00:00.000")
          assert(fst.getAs[String]("updatedAt") == "2024-02-10T09:30:00.000")
          assert(fst.getAs[String]("status") == "COMPLETED")
          assert(fst.getAs[String]("occurredOn") == "2024-02-11 13:10:00.000")
        }
      )
    }
  }

  test("handle CandidateSessionRecorderArchivedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "aggregateIdentifier",
        "loadtime",
        "id",
        "candidateId",
        "deliveryId",
        "assessmentId",
        "createdAt",
        "updatedAt",
        "status",
        "occurredOn",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"CandidateSessionRecorderArchivedIntegrationEvent",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |       "id": "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb",
                    |       "candidateId": "b87d65a8-bc17-41f4-b1a4-cd38c1f10be6",
                    |       "deliveryId": "3278a6e9-0b0b-437e-baae-024fa178b3b8",
                    |       "assessmentId": "8d829b85-54f4-4d82-8301-8fa3f9ad10f2",
                    |       "updatedAt": "2024-02-10T09:30:00.000",
                    |       "status": "COMPLETED",
                    |       "occurredOn": 1707657000000,
                    |       "aggregateIdentifier": "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb"
                    |     }
                    | },
                    | "timestamp": "2023-05-15 16:23:46.609"
                    |}
                    |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-candidate-session-recorder-archived-sink").map(_.input)
          assert(dfOpt.isDefined)
          val df = dfOpt.get

          assert(df.columns.toSet == expectedColumns)
          assert(df.collect().length == 1)

          val fst = df.first()
          assert(fst.getAs[String]("tenantId") == "93e4949d-7eff-4707-9201-dac917a5e013")
          assert(fst.getAs[String]("eventType") == "CandidateSessionRecorderArchivedIntegrationEvent")
          assert(fst.getAs[String]("id") == "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb")
          assert(fst.getAs[String]("candidateId") == "b87d65a8-bc17-41f4-b1a4-cd38c1f10be6")
          assert(fst.getAs[String]("deliveryId") == "3278a6e9-0b0b-437e-baae-024fa178b3b8")
          assert(fst.getAs[String]("assessmentId") == "8d829b85-54f4-4d82-8301-8fa3f9ad10f2")
          assert(fst.getAs[String]("updatedAt") == "2024-02-10T09:30:00.000")
          assert(fst.getAs[String]("status") == "COMPLETED")
          assert(fst.getAs[String]("occurredOn") == "2024-02-11 13:10:00.000")
        }
      )
    }
  }

  test("handle TestBlueprintDiscardedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "id",
        "status",
        "updatedBy",
        "updatedAt",
        "occurredOn",
        "aggregateIdentifier",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"TestBlueprintDiscardedIntegrationEvent",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |       "id": "discarded1",
                    |       "status": "DISCARDED",
                    |       "updatedBy": "user123",
                    |       "updatedAt": "2024-03-12T11:34:18Z",
                    |       "occurredOn": 1710243258020,
                    |       "aggregateIdentifier": "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb"
                    |     }
                    | },
                    | "timestamp": "2023-05-15 16:23:46.609"
                    |}
                    |]
        """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-blueprint-discarded-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "TestBlueprintDiscardedIntegrationEvent"
            fst.getAs[String]("id") shouldBe "discarded1"
            fst.getAs[String]("status") shouldBe "DISCARDED"
            fst.getAs[String]("updatedBy") shouldBe "user123"
            fst.getAs[String]("updatedAt") shouldBe "2024-03-12T11:34:18Z"
            fst.getAs[String]("occurredOn") shouldBe "2024-03-12 11:34:18.020"
            fst.getAs[String]("aggregateIdentifier") shouldBe "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb"
          }
        }
      )
    }
  }

  test("handle TestDiscardedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "testId",
        "status",
        "updatedBy",
        "updatedAt",
        "occurredOn",
        "aggregateIdentifier",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"TestDiscardedIntegrationEvent",
                    |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
                    |    },
                    |    "body":{
                    |       "testId": "test1",
                    |       "status": "DISCARDED",
                    |       "occurredOn": 1710243258020,
                    |       "updatedBy": "user123",
                    |       "updatedAt": "2024-03-12T11:34:18Z",
                    |       "aggregateIdentifier": "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb"
                    |     }
                    | },
                    | "timestamp": "2023-05-15 16:23:46.609"
                    |}
                    |]
        """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-discarded-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "TestDiscardedIntegrationEvent"
            fst.getAs[String]("testId") shouldBe "test1"
            fst.getAs[String]("status") shouldBe "DISCARDED"
            fst.getAs[String]("updatedBy") shouldBe "user123"
            fst.getAs[String]("updatedAt") shouldBe "2024-03-12T11:34:18Z"
            fst.getAs[String]("occurredOn") shouldBe "2024-03-12 11:34:18.020"
            fst.getAs[String]("aggregateIdentifier") shouldBe "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb"
          }
        }
      )
    }
  }

  test("handle TestArchivedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "testId",
        "status",
        "updatedBy",
        "updatedAt",
        "occurredOn",
        "aggregateIdentifier",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"TestArchivedIntegrationEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "testId": "test1",
              |       "status": "ARCHIVED",
              |       "occurredOn": 1710243258020,
              |       "updatedBy": "user123",
              |       "updatedAt": "2024-03-12T11:34:18Z",
              |       "aggregateIdentifier": "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb"
              |     }
              | },
              | "timestamp": "2023-05-15 16:23:46.609"
              |}
              |]
        """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-archived-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "TestArchivedIntegrationEvent"
            fst.getAs[String]("testId") shouldBe "test1"
            fst.getAs[String]("status") shouldBe "ARCHIVED"
            fst.getAs[String]("updatedBy") shouldBe "user123"
            fst.getAs[String]("updatedAt") shouldBe "2024-03-12T11:34:18Z"
            fst.getAs[String]("occurredOn") shouldBe "2024-03-12 11:34:18.020"
            fst.getAs[String]("aggregateIdentifier") shouldBe "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb"
          }
        }
      )
    }
  }

  test("handle TestBlueprintArchivedIntegrationEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "id",
        "status",
        "updatedBy",
        "updatedAt",
        "occurredOn",
        "aggregateIdentifier",
        "tenantId"
      )

      val fixtures = List(
        SparkFixture(
          key = TeacherTestIntegrationEventsSource,
          value =
            """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"TestBlueprintArchivedIntegrationEvent",
              |       "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
              |    },
              |    "body":{
              |       "id": "archived1",
              |       "status": "ARCHIVED",
              |       "updatedBy": "user123",
              |       "updatedAt": "2024-03-12T11:34:18Z",
              |       "occurredOn": 1710243258020,
              |       "aggregateIdentifier": "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb"
              |     }
              | },
              | "timestamp": "2023-05-15 16:23:46.609"
              |}
              |]
        """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "test-blueprint-archived-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "93e4949d-7eff-4707-9201-dac917a5e013"
            fst.getAs[String]("eventType") shouldBe "TestBlueprintArchivedIntegrationEvent"
            fst.getAs[String]("id") shouldBe "archived1"
            fst.getAs[String]("status") shouldBe "ARCHIVED"
            fst.getAs[String]("updatedBy") shouldBe "user123"
            fst.getAs[String]("updatedAt") shouldBe "2024-03-12T11:34:18Z"
            fst.getAs[String]("occurredOn") shouldBe "2024-03-12 11:34:18.020"
            fst.getAs[String]("aggregateIdentifier") shouldBe "e3d953ba-25d7-4e8b-9918-2c6dbfa7d6eb"
          }
        }
      )
    }
  }
}
