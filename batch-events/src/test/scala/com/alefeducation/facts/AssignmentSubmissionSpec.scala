package com.alefeducation.facts

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Helpers.{AssignmentSubmissionDeltaSink, AssignmentSubmissionRedshiftSink, AssignmentSubmissionSource}

class AssignmentSubmissionSpec extends SparkSuite {

  trait Setup {
    implicit val transformer: AssignmentSubmission = AssignmentSubmission(spark)
  }

  val expectedColumnsInDelta = Set(
    "assignment_submission_student_comment",
    "assignment_submission_id",
    "assignment_submission_assignment_id",
    "assignment_submission_assignment_instance_id",
    "assignment_submission_referrer_id",
    "assignment_submission_type",
    "assignment_submission_updated_on",
    "assignment_submission_returned_on",
    "assignment_submission_submitted_on",
    "assignment_submission_graded_on",
    "assignment_submission_evaluated_on",
    "assignment_submission_status",
    "assignment_submission_student_id",
    "assignment_submission_teacher_id",
    "assignment_submission_student_attachment_file_name",
    "assignment_submission_student_attachment_path",
    "assignment_submission_teacher_attachment_file_name",
    "assignment_submission_teacher_attachment_path",
    "assignment_submission_teacher_comment",
    "assignment_submission_teacher_score",
    "assignment_submission_tenant_id",
    "assignment_submission_date_dw_id",
    "assignment_submission_created_time",
    "assignment_submission_dw_created_time",
    "assignment_submission_resubmission_count",
    "eventdate"
  )

  val expectedColumnsInRedshift = Set(
    "assignment_submission_has_student_comment",
    "assignment_submission_id",
    "assignment_submission_assignment_id",
    "assignment_submission_assignment_instance_id",
    "assignment_submission_referrer_id",
    "assignment_submission_type",
    "assignment_submission_updated_on",
    "assignment_submission_returned_on",
    "assignment_submission_submitted_on",
    "assignment_submission_graded_on",
    "assignment_submission_evaluated_on",
    "assignment_submission_status",
    "assignment_submission_student_id",
    "assignment_submission_teacher_id",
    "assignment_submission_student_attachment_file_name",
    "assignment_submission_student_attachment_path",
    "assignment_submission_teacher_attachment_file_name",
    "assignment_submission_teacher_attachment_path",
    "assignment_submission_has_teacher_comment",
    "assignment_submission_teacher_score",
    "assignment_submission_tenant_id",
    "assignment_submission_date_dw_id",
    "assignment_submission_created_time",
    "assignment_submission_dw_created_time",
    "assignment_submission_resubmission_count",
    "eventdate"
  )
  test("transform AssignmentSubmissionMutatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = AssignmentSubmissionSource,
          value =
            """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "AssignmentSubmissionMutatedEvent",
              |  "id": "de630c58-9f11-44f8-a440-f6f740973b5e",
              |  "assignmentId": "assignmentId",
              |  "assignmentInstanceId": "assignmentInstanceId",
              |  "referrerId": "fe30b163-047c-4ec5-915b-c86102d60d37",
              |  "type": "TEACHER_ASSIGNMENT",
              |  "studentId": "5fc91352-6c16-4117-b03e-db307065344b",
              |  "teacherId": "e5e4b11a-026b-4c29-981f-7a5b2b30a0e6",
              |  "status": "GRADED",
              |  "studentAttachment": {
              |    "fileName": "sample.xlsx",
              |    "path": "assignments/submissions/7994d8a9-ce61-4e2a-bb69-da5901f3554c.xlsx"
              |  },
              |  "comment": "Comment is here",
              |  "createdOn": 1587459879743,
              |  "gradedOn": null,
              |  "evaluatedOn": 1587459901876,
              |  "updatedOn": 1587459888176,
              |  "submittedOn": 1587459896863,
              |  "returnedOn": 1587459908660,
              |  "teacherResponse": {
              |    "comment": "One two three",
              |    "score": 15,
              |    "attachment": {
              |       "fileName": "file-name11.txt",
              |       "path": "file-path11"
              |    },
              |    "teacherId": "e5e4b11a-026b-4c29-981f-7a5b2b30a0e6"
              |  },
              |  "occurredOn": "2020-02-27T04:09:10.861",
              |  "eventDateDw": "20200227"
              |}
                  """.stripMargin
        )
      )
      withSparkFixturesWithSink(
        fixtures,
        AssignmentSubmissionDeltaSink, { df =>
          assert(df.columns.toSet === expectedColumnsInDelta)
          assert[String](df, "assignment_submission_tenant_id", "tenant-id")
          assert[String](df, "assignment_submission_id", "de630c58-9f11-44f8-a440-f6f740973b5e")
          assert[String](df, "assignment_submission_assignment_id", "assignmentId")
          assert[String](df, "assignment_submission_referrer_id", "fe30b163-047c-4ec5-915b-c86102d60d37")
          assert[String](df, "assignment_submission_assignment_instance_id", "assignmentInstanceId")
          assert[String](df, "assignment_submission_type", "TEACHER_ASSIGNMENT")
          assert[String](df, "assignment_submission_student_id", "5fc91352-6c16-4117-b03e-db307065344b")
          assert[String](df, "assignment_submission_teacher_id", "e5e4b11a-026b-4c29-981f-7a5b2b30a0e6")
          assert[String](df, "assignment_submission_status", "GRADED")
          assert[String](df, "assignment_submission_student_comment", "Comment is here")
          assert[String](df, "assignment_submission_graded_on", null)
          assert[String](df, "assignment_submission_student_attachment_file_name", "sample.xlsx")
          assert[String](df,
            "assignment_submission_student_attachment_path",
            "assignments/submissions/7994d8a9-ce61-4e2a-bb69-da5901f3554c.xlsx")

          assert[String](df, "assignment_submission_teacher_comment", "One two three")
          assert[Double](df, "assignment_submission_teacher_score", 15)
          assert[String](df, "assignment_submission_teacher_id", "e5e4b11a-026b-4c29-981f-7a5b2b30a0e6")

          assert[String](df, "assignment_submission_teacher_attachment_path", "file-path11")

        }
      )

      withSparkFixturesWithSink(
        fixtures,
        AssignmentSubmissionRedshiftSink, { df =>
          assert(df.columns.toSet === expectedColumnsInRedshift)
          assert[String](df, "assignment_submission_tenant_id", "tenant-id")
          assert[String](df, "assignment_submission_id", "de630c58-9f11-44f8-a440-f6f740973b5e")
          assert[String](df, "assignment_submission_assignment_id", "assignmentId")
          assert[String](df, "assignment_submission_referrer_id", "fe30b163-047c-4ec5-915b-c86102d60d37")
          assert[String](df, "assignment_submission_assignment_instance_id", "assignmentInstanceId")
          assert[String](df, "assignment_submission_type", "TEACHER_ASSIGNMENT")
          assert[String](df, "assignment_submission_student_id", "5fc91352-6c16-4117-b03e-db307065344b")
          assert[String](df, "assignment_submission_teacher_id", "e5e4b11a-026b-4c29-981f-7a5b2b30a0e6")
          assert[String](df, "assignment_submission_status", "GRADED")
          assert[Boolean](df, "assignment_submission_has_student_comment", true)
          assert[String](df, "assignment_submission_graded_on", null)
          assert[String](df, "assignment_submission_student_attachment_file_name", "sample.xlsx")
          assert[String](df,
            "assignment_submission_student_attachment_path",
            "assignments/submissions/7994d8a9-ce61-4e2a-bb69-da5901f3554c.xlsx")

          assert[Boolean](df, "assignment_submission_has_teacher_comment", true)
          assert[Double](df, "assignment_submission_teacher_score", 15)
          assert[String](df, "assignment_submission_teacher_id", "e5e4b11a-026b-4c29-981f-7a5b2b30a0e6")

          assert[String](df, "assignment_submission_teacher_attachment_path", "file-path11")

        }
      )
    }
  }

}
