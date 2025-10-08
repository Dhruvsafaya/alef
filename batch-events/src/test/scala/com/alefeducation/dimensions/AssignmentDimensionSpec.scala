package com.alefeducation.dimensions

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers._

class AssignmentDimensionSpec extends SparkSuite {

  trait Setup {
    implicit val transformer = AssignmentDimension(spark)
  }

  test("teacher assignment create or update dimension") {
    new Setup {
      val source = AssignmentMutatedParquetSource
      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[{
                    |"tenantId":"tenantID",
                    |"eventType":"AssignmentCreatedEvent",
                    |"loadtime":"2020-02-19T07:44:02.099Z",
                    |"id":"assignmentId",
                    |"type":"TEACHER_ASSIGNMENT",
                    |"title":"assignment-title",
                    |"description":"assignment-desc",
                    |"maxScore":12.0,
                    |"attachment":
                    |{
                    |    "fileId":"1234",
                    |    "fileName":"sample.doc",
                    |    "path":"assignments/attachments/aaa.doc"
                    |},
                    |"isGradeable":true,
                    |"schoolId":"schoolId",
                    |"language":"ENGLISH",
                    |"status":"DRAFT",
                    |"createdBy":"teacherId",
                    |"updatedBy":"teacherId",
                    |"createdOn":1587382306099,
                    |"metadata" : null,
                    |"updatedOn":null,
                    |"publishedOn":null,
                    |"occurredOn":"2020-02-18 07:10:03.760",
                    |"eventDateDw":"20200218"
                    |},{
                    |"tenantId":"tenantID",
                    |"eventType":"AssignmentUpdatedEvent",
                    |"loadtime":"2020-02-19T07:44:05.128Z",
                    |"id":"assignmentId",
                    |"type":"TEACHER_ASSIGNMENT",
                    |"title":"assignment-title",
                    |"description":"assignment-desc",
                    |"maxScore":12.0,
                    |"attachment":
                    |{
                    |    "fileId":"1234",
                    |    "fileName":"sample.doc",
                    |    "path":"assignments/attachments/aaa.doc"
                    |},
                    |"isGradeable":true,
                    |"schoolId":"schoolId",
                    |"language":"ENGLISH",
                    |"status":"PUBLISHED",
                    |"attachmentRequired":"true",
                    |"commentRequired" : "false",
                    |"metadata" : null,
                    |"createdBy":"teacherId",
                    |"updatedBy":"teacherId",
                    |"createdOn":1587382306099,
                    |"updatedOn":1587457490883,
                    |"publishedOn":1587457490883,
                    |"occurredOn":"2020-02-18 07:10:03.841",
                    |"eventDateDw":"20200218"
                    |}]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedColumns = Set(
            "assignment_id",
            "assignment_type",
            "assignment_title",
            "assignment_description",
            "assignment_max_score",
            "assignment_attachment_file_name",
            "assignment_attachment_file_id",
            "assignment_attachment_path",
            "assignment_tenant_id",
            "assignment_school_id",
            "assignment_is_gradeable",
            "assignment_language",
            "assignment_assignment_status",
            "assignment_status",
            "assignment_attachment_required",
            "assignment_comment_required",
            "assignment_created_by",
            "assignment_updated_by",
            "assignment_published_on",
            "assignment_created_time",
            "assignment_dw_created_time",
            "assignment_updated_time",
            "assignment_deleted_time",
            "assignment_dw_updated_time"
          )

          val s3Sink: Sink = sinks.filter(_.name == AssignmentMutatedParquetSource).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2020-02-18")

          val deltaSinks = sinks.filter(_.name == AssignmentDeltaSink)
          val deltaCreateDf = deltaSinks.head.output.cache()
          assert(deltaCreateDf.count === 1)
          assert(deltaCreateDf.columns.toSet === expectedColumns)
          assert[String](deltaCreateDf, "assignment_id", "assignmentId")
          assert[String](deltaCreateDf, "assignment_title", "assignment-title")
          assert[String](deltaCreateDf, "assignment_description", "assignment-desc")
          assert[Double](deltaCreateDf, "assignment_max_score", 12.0)
          assert[String](deltaCreateDf, "assignment_attachment_file_name", "sample.doc")
          assert[String](deltaCreateDf, "assignment_school_id", "schoolId")
          assert[String](deltaCreateDf, "assignment_tenant_id", "tenantID")
          assert[String](deltaCreateDf, "assignment_language", "ENGLISH")
          assert[String](deltaCreateDf, "assignment_created_by", "teacherId")
          assert[String](deltaCreateDf, "assignment_updated_by", "teacherId")
          assert[String](deltaCreateDf, "assignment_assignment_status", "DRAFT")
          assert[String](deltaCreateDf, "assignment_published_on", null)
          assert[String](deltaCreateDf, "assignment_type", "TEACHER_ASSIGNMENT")
          assert[Int](deltaCreateDf, "assignment_status", 1)

          val deltaUpdateDf = deltaSinks.tail.head.output.cache()
          assert(deltaUpdateDf.count === 1)
          assert(deltaUpdateDf.columns.toSet === expectedColumns - "assignment_tenant_id" - "assignment_school_id")
          assert[String](deltaUpdateDf, "assignment_id", "assignmentId")
          assert[String](deltaUpdateDf, "assignment_title", "assignment-title")
          assert[String](deltaUpdateDf, "assignment_description", "assignment-desc")
          assert[Double](deltaUpdateDf, "assignment_max_score", 12.0)
          assert[String](deltaUpdateDf, "assignment_attachment_file_name", "sample.doc")
          assert[String](deltaUpdateDf, "assignment_language", "ENGLISH")
          assert[String](deltaUpdateDf, "assignment_created_by", "teacherId")
          assert[String](deltaUpdateDf, "assignment_updated_by", "teacherId")
          assert[String](deltaUpdateDf, "assignment_assignment_status", "PUBLISHED")
          assert[Int](deltaUpdateDf, "assignment_status", 1)
        }
      )
    }
  }

  test("assignment deleted dimension") {
    new Setup {
      val source = AssignmentDeletedParquetSource
      val fixtures = List(
        SparkFixture(
          key = source,
          value = """
                    |[{
                    |  "eventType":"AssignmentDeletedEvent",
                    |  "tenantId":"tenantId",
                    |  "id": "assignmentId",
                    |  "occurredOn": "2020-02-18 11:48:46"
                    |}]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedColumns = Set(
            "assignment_id",
            "assignment_status",
            "assignment_dw_created_time",
            "assignment_deleted_time",
            "assignment_dw_updated_time",
            "assignment_updated_time",
            "assignment_created_time"
          )
          val deltaSinks = sinks.filter(_.name == AssignmentDeltaSink)
          val df = deltaSinks.head.output.cache()
          assert(df.count === 1)
          assert(df.columns.toSet === expectedColumns - "assignment_school_id")
          assert[String](df, "assignment_id", "assignmentId")
          assert[Int](df, "assignment_status", 4)
        }
      )
    }
  }

  test("alef assignment create or update dimension") {
    new Setup {
      val source = AlefAssignmentMutatedParquetSource
      val fixtures = List(
        SparkFixture(
          key = source,
          value =
            """
                    |[{
                    |"eventType":"AssignmentCreatedEvent",
                    |"loadtime":"2020-02-19T07:44:02.099Z",
                    |"id":"assignmentId",
                    |"type":"ALEF_ASSIGNMENT",
                    |"title":"assignment-title",
                    |"description":"assignment-desc",
                    |"maxScore":12.0,
                    |"attachment":null,
                    |"isGradeable":true,
                    |"schoolId":"schoolId",
                    |"language":"ENGLISH",
                    |"status":"DRAFT",
                    |"createdBy":"teacherId",
                    |"updatedBy":"teacherId",
                    |"createdOn":1587382306099,
                    |"metadata": {
                    |    "keywords": [
                    |      "plates",
                    |      "volcanoes",
                    |      "continental crust"
                    |    ],
                    |    "resourceType": "PROJECT",
                    |    "summativeAssessment": true,
                    |    "difficultyLevel": "EASY",
                    |    "cognitiveDimensions": [
                    |      "REMEMBERING"
                    |    ],
                    |    "knowledgeDimensions": "FACTUAL",
                    |    "lexileLevel": "8",
                    |    "copyrights": [
                    |      "ALEF"
                    |    ],
                    |    "conditionsOfUse": [],
                    |    "formatType": "Text",
                    |    "author": "vq02_sc@test.com",
                    |    "authoredDate": "2020-08-24T00:00:00.000",
                    |    "language": "EN_US",
                    |    "curriculumOutcomes": [
                    |      {
                    |        "type": "sub_level",
                    |        "id": "962925",
                    |        "name": "MS-8-ESS1-C1- The History of Planet Earth",
                    |        "description": "Tectonic processes continually generate new ocean sea floor at ridges and destroy old sea floor at trenches. (HS.ESS1.C GBE),(secondary)\r\n\r\n",
                    |        "curriculum": "UAE MOE",
                    |        "grade": "8",
                    |        "subject": "Science"
                    |      }
                    |    ],
                    |    "skillIds": []
                    |  },
                    |"updatedOn":null,
                    |"publishedOn":null,
                    |"occurredOn":"2020-02-18 07:10:03.760",
                    |"eventDateDw":"20200218"
                    |},{
                    |"tenantId":"tenantID",
                    |"eventType":"AssignmentUpdatedEvent",
                    |"loadtime":"2020-02-19T07:44:05.128Z",
                    |"id":"assignmentId",
                    |"type":"ALEF_ASSIGNMENT",
                    |"title":"assignment-title",
                    |"description":"assignment-desc",
                    |"maxScore":12.0,
                    |"attachment":
                    |{
                    |    "fileId":"1234",
                    |    "fileName":"sample.doc",
                    |    "path":"assignments/attachments/aaa.doc"
                    |},
                    |"isGradeable":true,
                    |"schoolId":"schoolId",
                    |"language":"ENGLISH",
                    |"status":"PUBLISHED",
                    |"attachmentRequired":"true",
                    |"commentRequired" : "false",
                    |"metadata": {
                    |    "keywords": [
                    |      "plates",
                    |      "volcanoes",
                    |      "continental crust"
                    |    ],
                    |    "resourceType": "PROJECT",
                    |    "summativeAssessment": true,
                    |    "difficultyLevel": "EASY",
                    |    "cognitiveDimensions": [
                    |      "REMEMBERING"
                    |    ],
                    |    "knowledgeDimensions": "FACTUAL",
                    |    "lexileLevel": "8",
                    |    "copyrights": [
                    |      "ALEF"
                    |    ],
                    |    "conditionsOfUse": [],
                    |    "formatType": "Text",
                    |    "author": "vq02_sc@test.com",
                    |    "authoredDate": "2020-08-24T00:00:00.000",
                    |    "language": "EN_US",
                    |    "curriculumOutcomes": [
                    |      {
                    |        "type": "sub_level",
                    |        "id": "962925",
                    |        "name": "MS-8-ESS1-C1- The History of Planet Earth",
                    |        "description": "Tectonic processes continually generate new ocean sea floor at ridges and destroy old sea floor at trenches. (HS.ESS1.C GBE),(secondary)\r\n\r\n",
                    |        "curriculum": "UAE MOE",
                    |        "grade": "8",
                    |        "subject": "Science"
                    |      }
                    |    ],
                    |    "skillIds": []
                    |  },
                    |"createdBy":"teacherId",
                    |"updatedBy":"teacherId",
                    |"createdOn":1587382306099,
                    |"updatedOn":1587457490883,
                    |"publishedOn":1587457490883,
                    |"occurredOn":"2020-02-18 07:10:03.841",
                    |"eventDateDw":"20200218"
                    |}]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedColumns = Set(
            "assignment_id",
            "assignment_type",
            "assignment_title",
            "assignment_description",
            "assignment_max_score",
            "assignment_attachment_file_name",
            "assignment_attachment_file_id",
            "assignment_attachment_path",
            "assignment_school_id",
            "assignment_is_gradeable",
            "assignment_language",
            "assignment_assignment_status",
            "assignment_status",
            "assignment_attachment_required",
            "assignment_comment_required",
            "assignment_created_by",
            "assignment_updated_by",
            "assignment_published_on",
            "assignment_created_time",
            "assignment_dw_created_time",
            "assignment_updated_time",
            "assignment_deleted_time",
            "assignment_dw_updated_time",
            "assignment_metadata_keywords",
            "assignment_metadata_resource_type",
            "assignment_metadata_is_sa",
            "assignment_metadata_difficulty_level",
            "assignment_metadata_cognitive_dimensions",
            "assignment_metadata_knowledge_dimensions",
            "assignment_metadata_lexile_level",
            "assignment_metadata_copyrights",
            "assignment_metadata_conditions_of_use",
            "assignment_metadata_format_type",
            "assignment_metadata_author",
            "assignment_metadata_authored_date",
            "assignment_metadata_curriculum_outcomes",
            "assignment_metadata_skills",
            "assignment_metadata_language"
          )

          val expectedRedshiftColumns = expectedColumns - (
            "assignment_description",
            "assignment_metadata_keywords",
            "assignment_metadata_cognitive_dimensions",
            "assignment_metadata_copyrights",
            "assignment_metadata_conditions_of_use",
            "assignment_metadata_curriculum_outcomes",
            "assignment_metadata_skills"
          )

          val s3Sink: Sink = sinks.filter(_.name == AlefAssignmentMutatedParquetSource).head
          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2020-02-18")

          val deltaSinks = sinks.filter(_.name == AssignmentDeltaSink)
          val deltaCreateDf = deltaSinks.head.output.cache()
          assert(deltaCreateDf.count === 1)
          assert(deltaCreateDf.columns.toSet === expectedColumns)
          assert[String](deltaCreateDf, "assignment_id", "assignmentId")
          assert[String](deltaCreateDf, "assignment_title", "assignment-title")
          assert[String](deltaCreateDf, "assignment_description", "assignment-desc")
          assert[Double](deltaCreateDf, "assignment_max_score", 12.0)
          assert[String](deltaCreateDf, "assignment_attachment_file_name", null)
          assert[String](deltaCreateDf, "assignment_school_id", "schoolId")
          assert[String](deltaCreateDf, "assignment_language", "ENGLISH")
          assert[String](deltaCreateDf, "assignment_created_by", "teacherId")
          assert[String](deltaCreateDf, "assignment_updated_by", "teacherId")
          assert[String](deltaCreateDf, "assignment_assignment_status", "DRAFT")
          assert[String](deltaCreateDf, "assignment_published_on", null)
          assert[String](deltaCreateDf, "assignment_type", "ALEF_ASSIGNMENT")
          assert[Int](deltaCreateDf, "assignment_status", 1)

          val deltaUpdateDf = deltaSinks.tail.head.output.cache()
          assert(deltaUpdateDf.count === 1)
          assert(deltaUpdateDf.columns.toSet === expectedColumns - "assignment_school_id")
          assert[String](deltaUpdateDf, "assignment_id", "assignmentId")
          assert[String](deltaUpdateDf, "assignment_title", "assignment-title")
          assert[String](deltaUpdateDf, "assignment_description", "assignment-desc")
          assert[Double](deltaUpdateDf, "assignment_max_score", 12.0)
          assert[String](deltaUpdateDf, "assignment_attachment_file_name", "sample.doc")
          assert[String](deltaUpdateDf, "assignment_language", "ENGLISH")
          assert[String](deltaUpdateDf, "assignment_created_by", "teacherId")
          assert[String](deltaUpdateDf, "assignment_updated_by", "teacherId")
          assert[String](deltaUpdateDf, "assignment_assignment_status", "PUBLISHED")
          assert[Int](deltaUpdateDf, "assignment_status", 1)

          val redshiftSinks = sinks.filter(_.name == AssignmentRedshiftSink)
          val redshiftCreateDf = redshiftSinks.head.output.cache()
          assert(redshiftCreateDf.count === 1)
          assert(redshiftCreateDf.columns.toSet === expectedRedshiftColumns)
          assert[String](redshiftCreateDf, "assignment_id", "assignmentId")
          assert[String](redshiftCreateDf, "assignment_title", "assignment-title")
          assert[Double](redshiftCreateDf, "assignment_max_score", 12.0)
          assert[String](redshiftCreateDf, "assignment_attachment_file_name", null)
          assert[String](redshiftCreateDf, "assignment_school_id", "schoolId")
          assert[String](redshiftCreateDf, "assignment_language", "ENGLISH")
          assert[String](redshiftCreateDf, "assignment_created_by", "teacherId")
          assert[String](redshiftCreateDf, "assignment_updated_by", "teacherId")
          assert[String](redshiftCreateDf, "assignment_assignment_status", "DRAFT")
          assert[String](redshiftCreateDf, "assignment_published_on", null)
          assert[String](redshiftCreateDf, "assignment_type", "ALEF_ASSIGNMENT")
          assert[Int](redshiftCreateDf, "assignment_status", 1)

          val redshiftUpdateDf = redshiftSinks.tail.head.output.cache()
          assert(redshiftUpdateDf.count === 1)
          assert(redshiftUpdateDf.columns.toSet === expectedRedshiftColumns - "assignment_school_id")
          assert[String](redshiftUpdateDf, "assignment_id", "assignmentId")
          assert[String](redshiftUpdateDf, "assignment_title", "assignment-title")
          assert[Double](redshiftUpdateDf, "assignment_max_score", 12.0)
          assert[String](redshiftUpdateDf, "assignment_attachment_file_name", "sample.doc")
          assert[String](redshiftUpdateDf, "assignment_language", "ENGLISH")
          assert[String](redshiftUpdateDf, "assignment_created_by", "teacherId")
          assert[String](redshiftUpdateDf, "assignment_updated_by", "teacherId")
          assert[String](redshiftUpdateDf, "assignment_assignment_status", "PUBLISHED")
          assert[Int](redshiftUpdateDf, "assignment_status", 1)
        }
      )
    }
  }
}
