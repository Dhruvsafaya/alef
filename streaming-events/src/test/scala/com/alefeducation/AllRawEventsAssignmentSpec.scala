package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.functions.{col, explode}
import org.scalatest.matchers.should.Matchers

class AllRawEventsAssignmentSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer = new AllRawEvents(spark)
  }

  test("handle AssignmentCreatedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "type",
        "metadata",
        "eventDateDw",
        "createdOn",
        "maxScore",
        "updatedOn",
        "isGradeable",
        "description",
        "schoolId",
        "loadtime",
        "publishedOn",
        "occurredOn",
        "id",
        "language",
        "status",
        "createdBy",
        "attachment",
        "title",
        "tenantId",
        "updatedBy",
        "attachmentRequired",
        "commentRequired"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"AssignmentCreatedEvent",
              |       "tenantId":"tenantId"
              |    },
              |    "body":{
              |       "id": "assignmentId_1",
              |       "type":"TEACHER_ASSIGNMENT",
              |       "title": "assignment-title1",
              |       "description": "assignment-description1",
              |       "maxScore": 100.0,
              |       "attachment": {
              |           "fileName": "file-name1.txt",
              |           "path": "/upload-path1"
              |        },
              |        "isGradeable": true,
              |        "schoolId": "school-UUID",
              |        "language": "ENGLISH",
              |        "status": "DRAFT",
              |        "createdBy": "createdById",
              |        "updatedBy": "updatedById",
              |        "createdOn": 1582026526000,
              |        "updatedOn": 1582026526000,
              |        "publishedOn": 1582026526000,
              |        "metadata":null,
              |        "occurredOn": 1582026526000
              |     }
              | },
              | "timestamp": "2020-02-20 16:23:46.609"
              |}
              |,
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"AssignmentCreatedEvent",
              |       "tenantId":"tenantId"
              |    },
              |    "body":{
              |       "id": "assignmentId_2",
              |       "title": "assignment-title2",
              |       "type":"TEACHER_ASSIGNMENT",
              |       "description": "assignment-description2",
              |       "maxScore": 100.0,
              |       "attachment": {
              |           "fileName": "file-name2.txt",
              |           "path": "/upload-path2"
              |        },
              |        "isGradeable": true,
              |        "schoolId": "school-UUID",
              |        "language": "ARABIC",
              |        "status": "PUBLISHED",
              |        "createdBy": "createdById",
              |        "updatedBy": "updatedById",
              |        "createdOn": 1582026527000,
              |        "updatedOn": 1582026527000,
              |        "publishedOn": 1582026527000,
              |        "metadata":null,
              |        "occurredOn": 1582026527000
              |     }
              | },
              | "timestamp": "2020-02-20 16:23:56.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "assignment-mutated-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(2)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "AssignmentCreatedEvent"
            fst.getAs[String]("id") shouldBe "assignmentId_1"
            fst.getAs[String]("title") shouldBe "assignment-title1"
            fst.getAs[String]("description") shouldBe "assignment-description1"
            fst.getAs[Int]("maxScore") shouldBe 100.0
            fst.getAs[Boolean]("isGradeable") shouldBe true
            fst.getAs[String]("language") shouldBe "ENGLISH"
            fst.getAs[String]("status") shouldBe "DRAFT"
            fst.getAs[String]("createdBy") shouldBe "createdById"
            fst.getAs[Long]("createdOn") shouldBe 1582026526000L
            fst.getAs[Long]("updatedOn") shouldBe 1582026526000L
            fst.getAs[Long]("publishedOn") shouldBe 1582026526000L
            fst.getAs[String]("occurredOn") shouldBe "2020-02-18 11:48:46.000"

            val attachmentDf = df.select(col("attachment.*"))
            val attachmentRow = attachmentDf.first()
            attachmentRow.getAs[String]("fileName") shouldBe "file-name1.txt"
            attachmentRow.getAs[String]("path") shouldBe "/upload-path1"
          }
        }
      )
    }
  }

  test("handle AssignmentUpdatedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "type",
        "metadata",
        "eventDateDw",
        "createdOn",
        "maxScore",
        "updatedOn",
        "isGradeable",
        "description",
        "schoolId",
        "loadtime",
        "publishedOn",
        "occurredOn",
        "id",
        "language",
        "status",
        "createdBy",
        "attachment",
        "title",
        "tenantId",
        "updatedBy",
        "attachmentRequired",
        "commentRequired"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"AssignmentUpdatedEvent",
              |       "tenantId":"tenantId"
              |    },
              |    "body":{
              |       "id": "assignmentId_1",
              |       "title": "assignment-title1",
              |        "type":"TEACHER_ASSIGNMENT",
              |       "description": "assignment-description1",
              |       "maxScore": 100.0,
              |       "attachment": {
              |           "fileName": "file-name1.txt",
              |           "path": "/upload-path1"
              |        },
              |        "isGradeable": true,
              |        "schoolId": null,
              |        "language": "ENGLISH",
              |        "status": "DRAFT",
              |        "createdBy": "createdById",
              |        "updatedBy": "updatedById",
              |        "createdOn": 1582026526000,
              |        "updatedOn": 1582026526000,
              |        "publishedOn": 1582026526000,
              |        "metadata":null,
              |        "occurredOn": 1582026526000
              |     }
              | },
              | "timestamp": "2020-02-20 16:23:46.609"
              |}
              |,
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"AssignmentUpdatedEvent",
              |       "tenantId":"tenantId"
              |    },
              |    "body":{
              |       "id": "assignmentId_2",
              |       "title": "assignment-title2",
              |        "type":"TEACHER_ASSIGNMENT",
              |       "description": "assignment-description2",
              |       "maxScore": 100.0,
              |       "attachment": {
              |           "fileName": "file-name2.txt",
              |           "path": "/upload-path2"
              |        },
              |        "isGradeable": true,
              |        "schoolId": "school-UUID",
              |        "language": "ARABIC",
              |        "status": "PUBLISHED",
              |        "createdBy": "createdById",
              |        "updatedBy": "updatedById",
              |        "createdOn": 1582026527000,
              |        "updatedOn": 1582026527000,
              |        "publishedOn": 1582026527000,
              |        "metadata":null,
              |        "occurredOn": 1582026527000
              |     }
              | },
              | "timestamp": "2020-02-20 16:23:56.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "assignment-mutated-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(2)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "AssignmentUpdatedEvent"
            fst.getAs[String]("id") shouldBe "assignmentId_1"
            fst.getAs[String]("title") shouldBe "assignment-title1"
            fst.getAs[String]("description") shouldBe "assignment-description1"
            fst.getAs[Int]("maxScore") shouldBe 100.0
            fst.getAs[Boolean]("isGradeable") shouldBe true
            fst.getAs[String]("language") shouldBe "ENGLISH"
            fst.getAs[String]("status") shouldBe "DRAFT"
            fst.getAs[String]("createdBy") shouldBe "createdById"
            fst.getAs[Long]("createdOn") shouldBe 1582026526000L
            fst.getAs[Long]("updatedOn") shouldBe 1582026526000L
            fst.getAs[Long]("publishedOn") shouldBe 1582026526000L
            fst.getAs[String]("schoolId") shouldBe null
            fst.getAs[String]("occurredOn") shouldBe "2020-02-18 11:48:46.000"

            val attachmentDf = df.select(col("attachment.*"))
            val attachmentRow = attachmentDf.first()
            attachmentRow.getAs[String]("fileName") shouldBe "file-name1.txt"
            attachmentRow.getAs[String]("path") shouldBe "/upload-path1"
          }
        }
      )
    }
  }

  test("handle AssignmentInstanceCreatedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "createdOn",
        "teacherId",
        "updatedOn",
        "assignmentId",
        "students",
        "loadtime",
        "mloId",
        "allowLateSubmission",
        "levelId",
        "groupId",
        "classId",
        "subjectId",
        "occurredOn",
        "id",
        "schoolGradeId",
        "trimesterId",
        "dueOn",
        "tenantId",
        "type",
        "sectionId",
        "startOn",
        "k12Grade",
        "instructionalPlanId",
        "teachingPeriodId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"AssignmentInstanceCreatedEvent",
              |       "tenantId":"tenantId"
              |    },
              |    "body":{
              |       "id": "assignmentInstanceId1",
              |       "assignmentId": "assignmentId1",
              |       "dueOn": 1582026523000,
              |       "allowLateSubmission": true,
              |       "teachingPeriodId": "teachingPeriodId",
              |       "teacherId": "teacherId",
              |       "type": "MLO",
              |       "k12Grade": 6,
              |       "schoolGradeId": "gradeID",
              |       "classId": "classId",
              |       "subjectId": "subjectId",
              |       "sectionId": "sectionId",
              |       "groupId": "groupId",
              |       "levelId": "levelId",
              |       "mloId": "mloId",
              |       "trimesterId": "trimesterId",
              |       "createdOn": 1582026525000,
              |       "updatedOn": 0,
              |       "startOn": 1582026527000,
              |       "students": [
              |         {
              |           "id": "studentId1",
              |           "active": true
              |         }
              |       ],
              |       "occurredOn": 1582026527000
              |   }
              | },
              | "timestamp": "2020-02-20 16:23:46.609"
              |}
              |,
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"AssignmentInstanceCreatedEvent",
              |       "tenantId":"tenantId"
              |    },
              |    "body":{
              |       "id": "assignmentInstanceId2",
              |       "assignmentId": "assignmentId2",
              |       "dueOn": 1582026536000,
              |       "allowLateSubmission": true,
              |       "teacherId": "teacherId",
              |       "type": "GROUP",
              |       "k12Grade": 6,
              |       "schoolGradeId": "gradeID",
              |       "classId": "classId",
              |       "subjectId": "subjectId",
              |       "sectionId": "sectionId",
              |       "groupId": "groupId",
              |       "levelId": "levelId",
              |       "mloId": "mloId",
              |       "teachingPeriodId": "teachingPeriodId",
              |       "trimesterId": "trimesterId",
              |       "createdOn": 1582026537000,
              |       "updatedOn": 1582026538000,
              |       "startOn": 1582026539000,
              |       "students": [
              |         {
              |           "id": "studentId2",
              |           "active": true
              |         }
              |       ],
              |       "occurredOn": 1582026539000
              |   }
              | },
              | "timestamp": "2020-02-20 16:23:46.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "assignment-instance-mutated-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(2)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "AssignmentInstanceCreatedEvent"

            fst.getAs[String]("id") shouldBe "assignmentInstanceId1"
            fst.getAs[String]("assignmentId") shouldBe "assignmentId1"
            fst.getAs[Long]("dueOn") shouldBe 1582026523000L
            fst.getAs[Boolean]("allowLateSubmission") shouldBe true
            fst.getAs[String]("teacherId") shouldBe "teacherId"
            fst.getAs[String]("type") shouldBe "MLO"
            fst.getAs[Int]("k12Grade") shouldBe 6
            fst.getAs[String]("schoolGradeId") shouldBe "gradeID"
            fst.getAs[String]("classId") shouldBe "classId"
            fst.getAs[String]("subjectId") shouldBe "subjectId"
            fst.getAs[String]("sectionId") shouldBe "sectionId"
            fst.getAs[String]("groupId") shouldBe "groupId"
            fst.getAs[String]("levelId") shouldBe "levelId"
            fst.getAs[String]("mloId") shouldBe "mloId"
            fst.getAs[String]("trimesterId") shouldBe "trimesterId"
            fst.getAs[Long]("createdOn") shouldBe 1582026525000L
            fst.getAs[Long]("updatedOn") shouldBe 0
            fst.getAs[String]("occurredOn") shouldBe "2020-02-18 11:48:47.000"

            val studentRow = df.select(explode(col("students"))).select(col("col.*")).first()
            studentRow.getAs[String]("id") shouldBe "studentId1"
            studentRow.getAs[Boolean]("active") shouldBe true
          }
        }
      )
    }
  }

  test("handle AssignmentInstanceUpdatedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "createdOn",
        "teacherId",
        "updatedOn",
        "assignmentId",
        "students",
        "loadtime",
        "mloId",
        "allowLateSubmission",
        "levelId",
        "groupId",
        "classId",
        "subjectId",
        "occurredOn",
        "id",
        "schoolGradeId",
        "trimesterId",
        "dueOn",
        "tenantId",
        "type",
        "sectionId",
        "startOn",
        "k12Grade",
        "instructionalPlanId",
        "teachingPeriodId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"AssignmentInstanceUpdatedEvent",
              |       "tenantId":"tenantId"
              |    },
              |    "body":{
              |       "id": "assignmentInstanceId1",
              |       "assignmentId": "assignmentId1",
              |       "dueOn": 1582026523,
              |       "allowLateSubmission": true,
              |       "teacherId": "teacherId",
              |       "type": "MLO",
              |       "k12Grade": 6,
              |       "teachingPeriodId": "teachingPeriodId",
              |       "schoolGradeId": "gradeID",
              |       "classId": null,
              |       "subjectId": null,
              |       "sectionId": null,
              |       "groupId": "groupId",
              |       "levelId": "levelId",
              |       "mloId": "mloId",
              |       "trimesterId": "trimesterId",
              |       "createdOn": 1582026525000,
              |       "updatedOn": 1582026526000,
              |       "startOn": 1582026527000,
              |       "occurredOn": 1582026527000
              |   }
              | },
              | "timestamp": "2020-02-20 16:23:46.609"
              |}
              |,
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"AssignmentInstanceUpdatedEvent",
              |       "tenantId":"tenantId"
              |    },
              |    "body":{
              |       "id": "assignmentInstanceId2",
              |       "assignmentId": "assignmentId2",
              |       "dueOn": 1582026536,
              |       "allowLateSubmission": true,
              |       "teachingPeriodId": "teachingPeriodId",
              |       "teacherId": "teacherId",
              |       "type": "GROUP",
              |       "k12Grade": 6,
              |       "schoolGradeId": "gradeID",
              |       "classId": "classId",
              |       "subjectId": "subjectId",
              |       "sectionId": "sectionId",
              |       "groupId": "groupId",
              |       "levelId": "levelId",
              |       "mloId": "mloId",
              |       "trimesterId": "trimesterId",
              |       "createdOn": 1582026537000,
              |       "updatedOn": 1582026538000,
              |       "startOn": 1582026539000,
              |       "occurredOn": 1582026539000
              |   }
              | },
              | "timestamp": "2020-02-20 16:23:46.609"
              |}
              |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "assignment-instance-mutated-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(2)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "AssignmentInstanceUpdatedEvent"

            fst.getAs[String]("id") shouldBe "assignmentInstanceId1"
            fst.getAs[String]("assignmentId") shouldBe "assignmentId1"
            fst.getAs[Long]("dueOn") shouldBe 1582026523
            fst.getAs[Boolean]("allowLateSubmission") shouldBe true
            fst.getAs[String]("teacherId") shouldBe "teacherId"
            fst.getAs[String]("type") shouldBe "MLO"
            fst.getAs[Int]("k12Grade") shouldBe 6
            fst.getAs[String]("schoolGradeId") shouldBe "gradeID"
            fst.getAs[String]("classId") shouldBe null
            fst.getAs[String]("subjectId") shouldBe null
            fst.getAs[String]("sectionId") shouldBe null
            fst.getAs[String]("groupId") shouldBe "groupId"
            fst.getAs[String]("levelId") shouldBe "levelId"
            fst.getAs[String]("mloId") shouldBe "mloId"
            fst.getAs[String]("trimesterId") shouldBe "trimesterId"
            fst.getAs[Long]("createdOn") shouldBe 1582026525000L
            fst.getAs[Long]("updatedOn") shouldBe 1582026526000L
            fst.getAs[String]("occurredOn") shouldBe "2020-02-18 11:48:47.000"
            fst.getAs[List[String]]("students") shouldBe null
          }
        }
      )
    }
  }

  test("handle AssignmentDeletedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "loadtime",
        "occurredOn",
        "id",
        "tenantId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"AssignmentDeletedEvent",
              |       "tenantId":"tenantId"
              |    },
              |    "body":{
              |        "id": "assignmentId1",
              |        "occurredOn": 1582026526000
              |     }
              | },
              | "timestamp": "2020-02-20 16:23:46.609"
              |}
              |,
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"AssignmentDeletedEvent",
              |       "tenantId":"tenantId"
              |    },
              |    "body":{
              |        "id": "assignmentId2",
              |        "occurredOn": 1582026526000
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
          val dfOpt = sinks.find(_.name == "assignment-deleted-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(2)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "AssignmentDeletedEvent"
            fst.getAs[String]("id") shouldBe "assignmentId1"
            fst.getAs[String]("occurredOn") shouldBe "2020-02-18 11:48:46.000"
          }
        }
      )
    }
  }

  test("handle AssignmentInstanceDeletedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "assignmentId",
        "loadtime",
        "occurredOn",
        "id",
        "tenantId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"AssignmentInstanceDeletedEvent",
              |       "tenantId":"tenantId"
              |    },
              |    "body":{
              |        "id": "assignmentInstanceId1",
              |        "assignmentId": "assignmentId1",
              |        "occurredOn": 1582026526000
              |     }
              | },
              | "timestamp": "2020-02-20 16:23:46.609"
              |}
              |,
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"AssignmentInstanceDeletedEvent",
              |       "tenantId":"tenantId"
              |    },
              |    "body":{
              |        "id": "assignmentId2",
              |        "assignmentId": "assignmentInstanceId2",
              |        "occurredOn": 1582026526000
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
          val dfOpt = sinks.find(_.name == "assignment-instance-deleted-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(2)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "AssignmentInstanceDeletedEvent"
            fst.getAs[String]("id") shouldBe "assignmentInstanceId1"
            fst.getAs[String]("assignmentId") shouldBe "assignmentId1"
            fst.getAs[String]("occurredOn") shouldBe "2020-02-18 11:48:46.000"
          }
        }
      )
    }
  }

  test("handle AssignmentSubmissionMutatedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "createdOn",
        "teacherId",
        "updatedOn",
        "loadtime",
        "occurredOn",
        "id",
        "submittedOn",
        "status",
        "comment",
        "tenantId",
        "teacherResponse",
        "gradedOn",
        "studentId",
        "returnedOn",
        "studentAttachment",
        "assignmentId",
        "assignmentInstanceId",
        "referrerId",
        "type",
        "resubmissionCount",
        "evaluatedOn"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"AssignmentSubmissionMutatedEvent",
              |       "tenantId":"tenantId"
              |    },
              |    "body":{
              |       "id": "submissionId1",
              |       "assignmentId":"assignmentId",
              |       "referrerId":"referrerId",
              |       "type":"TEACHER_ASSIGNMENT",
              |       "evaluatedOn":1582026527000,
              |       "studentId": "studentId",
              |       "teacherId": "teacherId",
              |       "status": "PENDING",
              |       "studentAttachment": {
              |           "fileName": "file-name1.txt",
              |           "path": "file-path1"
              |        },
              |       "comment": "student-comment",
              |       "createdOn": 1582026527000,
              |       "updatedOn": 1582026527000,
              |       "submittedOn": 1582026527000,
              |       "returnedOn": 1582026527000,
              |       "gradedOn": 1582026527000,
              |       "teacherResponse": {
              |           "comment": "teacher-comment1",
              |           "score": 20,
              |           "attachment": {
              |             "fileName": "file-name11.txt",
              |             "path": "file-path11"
              |           }
              |        },
              |       "resubmissionCount": 1,
              |       "occurredOn": 1582026527000
              |     }
              | },
              | "timestamp": "2020-02-20 16:23:46.609"
              |},
              |{
              | "key": "key1",
              | "value":{
              |    "headers":{
              |       "eventType":"AssignmentSubmissionMutatedEvent",
              |       "tenantId":"tenantId"
              |    },
              |    "body":{
              |       "id": "submissionId2",
              |       "studentId": "studentId",
              |       "teacherId": "teacherId",
              |       "status": "UNGRADED",
              |       "studentAttachment": {
              |           "fileName": "file-name2.txt",
              |           "path": "file-path2"
              |        },
              |       "comment": "student-comment",
              |       "createdOn": 1582026527000,
              |       "updatedOn": 1582026527000,
              |       "submittedOn": 1582026527000,
              |       "returnedOn": 1582026527000,
              |       "gradedOn": 1582026527000,
              |       "teacherResponse": {
              |           "comment": "teacher-comment2",
              |           "score": 30,
              |           "attachment": {
              |             "fileName": "file-name22.txt",
              |             "path": "file-path22"
              |           }
              |        },
              |       "resubmissionCount": 2,
              |       "occurredOn": 1582026527000
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
          val dfOpt = sinks.find(_.name == "assignment-submission-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(2)
          dfOpt.foreach { df =>
            val fst = df.first()

            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "AssignmentSubmissionMutatedEvent"
            fst.getAs[String]("id") shouldBe "submissionId1"
            fst.getAs[Long]("evaluatedOn") shouldBe 1582026527000L
            fst.getAs[String]("referrerId") shouldBe "referrerId"
            fst.getAs[String]("type") shouldBe "TEACHER_ASSIGNMENT"
            fst.getAs[String]("studentId") shouldBe "studentId"
            fst.getAs[String]("teacherId") shouldBe "teacherId"
            fst.getAs[String]("status") shouldBe "PENDING"
            fst.getAs[String]("comment") shouldBe "student-comment"
            fst.getAs[Long]("createdOn") shouldBe 1582026527000L
            fst.getAs[Long]("updatedOn") shouldBe 1582026527000L
            fst.getAs[Long]("submittedOn") shouldBe 1582026527000L
            fst.getAs[Long]("returnedOn") shouldBe 1582026527000L
            fst.getAs[Long]("gradedOn") shouldBe 1582026527000L
            fst.getAs[Long]("resubmissionCount") shouldBe 1
            fst.getAs[String]("occurredOn") shouldBe "2020-02-18 11:48:47.000"

            val studentAttachmentDf = df.select(col("studentAttachment.*"))
            val studentAttachmentRow = studentAttachmentDf.first()
            studentAttachmentRow.getAs[String]("fileName") shouldBe "file-name1.txt"
            studentAttachmentRow.getAs[String]("path") shouldBe "file-path1"

            val teacherResponseDf = df.select(col("teacherResponse.*"))
            val teacherResponseRow = teacherResponseDf.first()
            teacherResponseRow.getAs[String]("comment") shouldBe "teacher-comment1"
            teacherResponseRow.getAs[Double]("score") shouldBe 20
            val attachmentDf = teacherResponseDf.select(col("attachment.*"))
            val attachmentRow = attachmentDf.first()
            attachmentRow.getAs[String]("fileName") shouldBe "file-name11.txt"
            attachmentRow.getAs[String]("path") shouldBe "file-path11"
          }
        }
      )
    }
  }

  test("handle AssignmentInstanceStudentsUpdatedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "students",
        "loadtime",
        "occurredOn",
        "id",
        "tenantId"
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"AssignmentInstanceStudentsUpdatedEvent",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |       "id": "assignmentInstanceId1",
                    |       "students": [
                    |       {
                    |         "id":"student1",
                    |         "active":true
                    |       },
                    |       {
                    |         "id":"student2",
                    |         "active":true
                    |       }],
                    |       "occurredOn": 1582026527000
                    |   }
                    | },
                    | "timestamp": "2020-02-20 16:23:46.609"
                    |}
                    |,
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"AssignmentInstanceStudentsUpdatedEvent",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |       "id": "assignmentInstanceId2",
                           "students": [
                    |       {
                    |         "id":"student21",
                    |         "active":true
                    |       },
                    |       {
                    |         "id":"student22",
                    |         "active":true
                    |       }],
                    |       "occurredOn": 1582026539000
                    |   }
                    | },
                    | "timestamp": "2020-02-20 16:23:46.609"
                    |}
                    |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "assignment-instance-students-updated-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(2)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "AssignmentInstanceStudentsUpdatedEvent"

            fst.getAs[String]("id") shouldBe "assignmentInstanceId1"
            fst.getAs[String]("occurredOn") shouldBe "2020-02-18 11:48:47.000"

            val studentRow = df.select(explode(col("students"))).select(col("col.*")).first()
            studentRow.getAs[String]("id") shouldBe "student1"
            studentRow.getAs[Boolean]("active") shouldBe true
          }
        }
      )
    }
  }

  test("handle AssignmentResubmissionRequestedEvent events") {
    new Setup {
      val expectedColumns: Set[String] = Set(
        "eventType",
        "eventDateDw",
        "tenantId",
        "occurredOn",
        "loadtime",
        "assignmentId",
        "studentId",
        "assignmentInstanceId",
        "classId",
        "mloId",
        "message",
        "teacherId",
        "resubmissionCount",
      )
      val fixtures = List(
        SparkFixture(
          key = "raw-events-source",
          value = """
                    |[
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"AssignmentResubmissionRequestedEvent",
                    |       "tenantId":"tenantId"
                    |    },
                    |    "body":{
                    |       "assignmentId": "assignmentId1",
                    |       "assignmentInstanceId": "assignmentInstanceId1",
                    |       "studentId": "student1",
                    |       "classId": "classId1",
                    |       "mloId": "mloId1",
                    |       "message": "message1",
                    |       "teacherId": "teacherId1",
                    |       "resubmissionCount": 1,
                    |       "occurredOn": 1582026527000
                    |   }
                    | },
                    | "timestamp": "2020-02-20 16:23:46.609"
                    |}
                    |,
                    |{
                    | "key": "key1",
                    | "value":{
                    |    "headers":{
                    |       "eventType":"AssignmentResubmissionRequestedEvent",
                    |       "tenantId":"tenantId"
                    |    },
                        "body":{
                    |       "assignmentId": "assignmentId2",
                    |       "assignmentInstanceId": "assignmentInstanceId2",
                    |       "studentId": "student2",
                    |       "classId": "classId2",
                    |       "mloId": "mloId2",
                    |       "message": "message2",
                    |       "teacherId": "teacherId2",
                    |       "resubmissionCount": 2,
                    |       "occurredOn": 1582026627000
                    |   }
                    | },
                    | "timestamp": "2020-02-20 16:23:46.609"
                    |}
                    |]
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val dfOpt = sinks.find(_.name == "assignment-resubmission-requested-sink").map(_.input)
          dfOpt.map(_.columns.toSet) shouldBe Some(expectedColumns)

          dfOpt.map(_.collect().length) shouldBe Some(2)
          dfOpt.foreach { df =>
            val fst = df.first()
            fst.getAs[String]("tenantId") shouldBe "tenantId"
            fst.getAs[String]("eventType") shouldBe "AssignmentResubmissionRequestedEvent"

            fst.getAs[String]("assignmentId") shouldBe "assignmentId1"
            fst.getAs[String]("assignmentInstanceId") shouldBe "assignmentInstanceId1"
            fst.getAs[String]("studentId") shouldBe "student1"
            fst.getAs[String]("classId") shouldBe "classId1"
            fst.getAs[String]("mloId") shouldBe "mloId1"
            fst.getAs[String]("message") shouldBe "message1"
            fst.getAs[String]("teacherId") shouldBe "teacherId1"
            fst.getAs[Long]("resubmissionCount") shouldBe 1L
            fst.getAs[String]("occurredOn") shouldBe "2020-02-18 11:48:47.000"
          }
        }
      )
    }
  }
}
