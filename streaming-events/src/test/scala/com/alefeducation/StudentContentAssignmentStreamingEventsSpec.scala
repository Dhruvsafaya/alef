package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.scalatest.matchers.should.Matchers

class StudentContentAssignmentStreamingEventsSpec extends SparkSuite with Matchers  {

  import com.alefeducation.bigdata.commons.testutils.ExpectedFields._

  val TeamMutatedSink = "team-mutated-sink"

  val TeamMutatedExpectedFields = List(
    ExpectedField(name = "tenantId", dataType = StringType),
    ExpectedField(name = "eventType", dataType = StringType),
    ExpectedField(name = "loadtime", dataType = StringType),
    ExpectedField(name = "teamId", dataType = StringType),
    ExpectedField(name = "name", dataType = StringType),
    ExpectedField(name = "description", dataType = StringType),
    ExpectedField(name = "classId", dataType = StringType),
    ExpectedField(name = "teacherId", dataType = StringType),
    ExpectedField(name = "occurredOn", dataType = StringType),
    ExpectedField(name = "eventDateDw", dataType = StringType)
  )

  val TeamDeletedSink = "team-deleted-sink"

  val TeamDeletedExpectedFields = List(
    ExpectedField(name = "tenantId", dataType = StringType),
    ExpectedField(name = "eventType", dataType = StringType),
    ExpectedField(name = "loadtime", dataType = StringType),
    ExpectedField(name = "teamId", dataType = StringType),
    ExpectedField(name = "occurredOn", dataType = StringType),
    ExpectedField(name = "eventDateDw", dataType = StringType)
  )

  val TeamMembersUpdatedSink = "team-members-updated-sink"

  val TeamMembersUpdatedExpectedFields = List(
    ExpectedField(name = "tenantId", dataType = StringType),
    ExpectedField(name = "eventType", dataType = StringType),
    ExpectedField(name = "loadtime", dataType = StringType),
    ExpectedField(name = "teamId", dataType = StringType),
    ExpectedField(name = "students", dataType = ArrayType(StringType)),
    ExpectedField(name = "occurredOn", dataType = StringType),
    ExpectedField(name = "eventDateDw", dataType = StringType)
  )

  val commonColumns: Set[String] = Set(
    "eventType",
    "eventDateDw",
    "loadtime",
    "occurredOn",
    "tenantId",
    "classId",
    "contentId",
    "contentType",
    "assignedBy",
    "mloId"
  )
  val expectedStudentContentColumns: Set[String] = commonColumns + "studentId"
  val expectedClassContentColumns: Set[String] = commonColumns

  trait Setup {
    implicit val transformer: StudentContentAssignmentStreamingEvents = new StudentContentAssignmentStreamingEvents(spark)
  }

  test("TeamCreatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "student-content-assignment-source",
          value = """[
              |  {
              |    "key": "key1",
              |    "value": {
              |      "headers": {
              |        "eventType": "TeamCreatedEvent",
              |        "tenantId": "someTenantId"
              |      },
              |      "body": {
              |        "classId": "4a4e9863-0980-4a4e-9863-78be6607d7fb",
              |        "contentId": "613768fa-0980-4a4e-9863-78be6607d7fb",
              |        "studentId": "86fcf7ae-8ec5-43a7-9a83-5142b0b16b1f",
              |        "occurredOn": 1592122860957
              |      }
              |    },
              |    "timestamp": "2020-08-05 16:23:46.609"
              |  }
              |]
              |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks.find(_.name == TeamMutatedSink).map(_.input).get
          assertExpectedFields(df.schema.fields.toList, TeamMutatedExpectedFields)
        }
      )
    }
  }

  test("TeamUpdatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "student-content-assignment-source",
          value = """
                    |[
                    |  {
                    |    "key": "key1",
                    |    "value": {
                    |      "headers": {
                    |        "eventType": "TeamUpdatedEvent",
                    |        "tenantId": "someTenantId"
                    |      },
                    |      "body": {
                    |        "classId": "4a4e9863-0980-4a4e-9863-78be6607d7fb",
                    |        "contentId": "613768fa-0980-4a4e-9863-78be6607d7fb",
                    |        "studentId": "86fcf7ae-8ec5-43a7-9a83-5142b0b16b1f",
                    |        "occurredOn": 1592122860957
                    |      }
                    |    },
                    |    "timestamp": "2020-08-05 16:23:46.609"
                    |  }
                    |]
                    |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks.find(_.name == TeamMutatedSink).map(_.input).get
          assertExpectedFields(df.schema.fields.toList, TeamMutatedExpectedFields)
        }
      )
    }
  }

  test("TeamDeletedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "student-content-assignment-source",
          value = """
                    |[
                    |  {
                    |    "key": "key1",
                    |    "value": {
                    |      "headers": {
                    |        "eventType": "TeamDeletedEvent",
                    |        "tenantId": "someTenantId"
                    |      },
                    |      "body": {
                    |        "teamId": "2ad0d839-8ae9-41c9-b7ac-07ecc341055f",
                    |        "occurredOn": 1592122860957
                    |      }
                    |    },
                    |    "timestamp": "2020-08-05 16:23:46.609"
                    |  }
                    |]
                    |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks.find(_.name == TeamDeletedSink).map(_.input).get
          assertExpectedFields(df.schema.fields.toList, TeamDeletedExpectedFields)
        }
      )
    }
  }

  test("TeamMembersUpdatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "student-content-assignment-source",
          value = """
              |[
              |  {
              |    "key": "key1",
              |    "value": {
              |      "headers": {
              |        "eventType": "TeamMembersUpdatedEvent",
              |        "tenantId": "someTenantId"
              |      },
              |      "body": {
              |        "teamId": "2ad0d839-8ae9-41c9-b7ac-07ecc341055f",
              |        "students": [
              |          "ea60347f-ebd5-44c1-a24f-5ead3aa1681c",
              |          "94230ddc-2198-4fa4-9cfc-bfe53d6208fa",
              |          "ea60347f-ebd5-44c1-a24f-5ead3aa1682c"
              |        ],
              |        "occurredOn": 1592122860957
              |      }
              |    },
              |    "timestamp": "2020-08-05 16:23:46.609"
              |  }
              |]
              |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val df = sinks.find(_.name == TeamMembersUpdatedSink).map(_.input).get
          assertExpectedFields(df.schema.fields.toList, TeamMembersUpdatedExpectedFields)
        }
      )
    }
  }

  test("handle Student Content Assignment events") {
    new Setup {

      val fixtures = List(
        SparkFixture(
          key = "student-content-assignment-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"ContentAssignedEvent",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |       "classId": "4a4e9863-0980-4a4e-9863-78be6607d7fb",
                    |       "contentId": "613768fa-0980-4a4e-9863-78be6607d7fb",
                    |       "contentType": "TEQ_1",
                    |       "studentId": "student1",
                    |        "occurredOn": 1592122860957
                    |     }
                    | },
                    | "timestamp": "2020-02-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "student-content-assignment-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedStudentContentColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "ContentAssignedEvent"
            fst.getAs[String]("classId") shouldBe "4a4e9863-0980-4a4e-9863-78be6607d7fb"
            fst.getAs[String]("contentId") shouldBe "613768fa-0980-4a4e-9863-78be6607d7fb"
            fst.getAs[String]("contentType") shouldBe "TEQ_1"
            fst.getAs[String]("studentId") shouldBe "student1"
          }
        }
      )
    }
  }

  test("handle Class Content Assignment events") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "student-content-assignment-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"ClassContentAssignedEvent",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |       "classId": "4a4e9863-0980-4a4e-9863-78be6607d7fb",
                    |       "contentId": "613768fa-0980-4a4e-9863-78be6607d7fb",
                    |       "contentType": "TEQ_1",
                    |       "mloId": "613768fa-0980-4a4e-9863-78be6607d7fb",
                    |       "assignedBy": "teacher1",
                    |       "occurredOn": 1592122860957
                    |     }
                    | },
                    | "timestamp": "2020-02-20 16:23:46.609"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "class-content-assignment-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedClassContentColumns)

          dfOpt.map(_.collect().length) shouldBe Some(1)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "ClassContentAssignedEvent"
            fst.getAs[String]("classId") shouldBe "4a4e9863-0980-4a4e-9863-78be6607d7fb"
            fst.getAs[String]("contentType") shouldBe "TEQ_1"
            fst.getAs[String]("contentId") shouldBe "613768fa-0980-4a4e-9863-78be6607d7fb"
            fst.getAs[String]("assignedBy") shouldBe "teacher1"
          }
        }
      )
    }
  }

}
