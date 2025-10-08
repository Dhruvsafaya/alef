package com.alefeducation.facts

import com.alefeducation.bigdata.batch.delta.DeltaCreateSink
import com.alefeducation.bigdata.commons.testutils.{ExpectedFields, SparkSuite}
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.types.StringType

class LessonFeedbackSpec extends SparkSuite {

  trait Setup {
    implicit val transformer = new LessonFeedbackTransformer("lesson-feedback", spark)
  }
  import ExpectedFields._

  val expectedColumns = Set(
    "lesson_feedback_id",
    "lesson_feedback_created_time",
    "lesson_feedback_dw_created_time",
    "lesson_feedback_date_dw_id",
    "tenant_uuid",
    "school_uuid",
    "academic_year_uuid",
    "class_uuid",
    "grade_uuid",
    "section_uuid",
    "subject_uuid",
    "student_uuid",
    "lo_uuid",
    "curr_uuid",
    "curr_grade_uuid",
    "curr_subject_uuid",
    "fle_ls_uuid",
    "lesson_feedback_trimester_id",
    "lesson_feedback_trimester_order",
    "lesson_feedback_content_academic_year",
    "lesson_feedback_rating",
    "lesson_feedback_rating_text",
    "lesson_feedback_has_comment",
    "lesson_feedback_is_cancelled",
    "lesson_feedback_learning_path_id",
    "lesson_feedback_teaching_period_id",
    "lesson_feedback_teaching_period_title",
    "lesson_feedback_instructional_plan_id"
  )

  test("transform lesson feedback events successfully") {

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetLessonFeedbackSource,
          value = """
              |[{
              |  "occurredOn": "2018-09-08 02:38:00.0",
              |  "tenantId": "tenant-id",
              |  "eventType": "LessonFeedbackSubmitted",
              |  "feedbackId":"feedback-id-1",
              |  "schoolId":"school-id-1",
              |  "academicYearId":"ay-id-1",
              |  "classId":"class-id-1",
              |  "gradeId":"grade-id-1",
              |  "section":"section-id-1",
              |  "subjectId":"subject-id-1",
              |  "learnerId":"learner-id-1",
              |  "learningSessionId":"ls-id-1",
              |  "learningObjectiveId":"lo-id-1",
              |  "trimesterId":"trimester-id-1",
              |  "trimesterOrder":1,
              |  "curriculumId":"curr-id-1",
              |  "curriculumSubjectId":"curr-sub-1",
              |  "curriculumGradeId":"curr-grade-1",
              |  "contentAcademicYear":"2020",
              |  "rating":3,
              |  "ratingText":"Neutral",
              |  "comment":"Good",
              |  "instructionalPlanId":"ip1",
              |  "learningPathId":"lp1",
              |  "teachingPeriodId": "teaching-period-id-1",
              |  "teachingPeriodTitle": "teaching-period-title-1",
              |  "eventDateDw": "20181029"
              |},
              |{
              |  "occurredOn": "2018-09-08 02:38:01.0",
              |  "tenantId": "tenant-id",
              |  "eventType": "LessonFeedbackCancelled",
              |  "feedbackId":"feedback-id-2",
              |  "schoolId":"school-id-1",
              |  "academicYearId":"ay-id-1",
              |  "classId":"class-id-1",
              |  "gradeId":"grade-id-1",
              |  "section":"section-id-1",
              |  "subjectId":"subject-id-1",
              |  "learnerId":"learner-id-1",
              |  "learningSessionId":"ls-id-1",
              |  "learningObjectiveId":"lo-id-1",
              |  "trimesterId":"trimester-id-1",
              |  "trimesterOrder":1,
              |  "curriculumId":"curr-id-1",
              |  "curriculumSubjectId":"curr-sub-1",
              |  "curriculumGradeId":"curr-grade-1",
              |  "contentAcademicYear":"2020",
              |  "rating":null,
              |  "ratingText":null,
              |  "comment":null,
              |  "instructionalPlanId":null,
              |  "learningPathId":null,
              |  "teachingPeriodId": null,
              |  "teachingPeriodTitle": null,
              |  "eventDateDw": "20181029"
              |}],
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 3)
          val df = sinks.filter(_.name == RedshiftLessonFeedbackSink).head.output

          assert(df.count === 2)
          assert(df.columns.toSet === expectedColumns)

          val feedbackSubmitted = df.filter("lesson_feedback_is_cancelled = false")
          assert[String](feedbackSubmitted, "lesson_feedback_id", "feedback-id-1")
          assert[String](feedbackSubmitted, "lesson_feedback_date_dw_id", "20181029")
          assert[String](feedbackSubmitted, "tenant_uuid", "tenant-id")
          assert[String](feedbackSubmitted, "school_uuid", "school-id-1")
          assert[String](feedbackSubmitted, "academic_year_uuid", "ay-id-1")
          assert[String](feedbackSubmitted, "grade_uuid", "grade-id-1")
          assert[String](feedbackSubmitted, "section_uuid", "section-id-1")
          assert[String](feedbackSubmitted, "subject_uuid", "subject-id-1")
          assert[String](feedbackSubmitted, "student_uuid", "learner-id-1")
          assert[String](feedbackSubmitted, "class_uuid", "class-id-1")
          assert[String](feedbackSubmitted, "lo_uuid", "lo-id-1")
          assert[String](feedbackSubmitted, "curr_uuid", "curr-id-1")
          assert[String](feedbackSubmitted, "curr_grade_uuid", "curr-grade-1")
          assert[String](feedbackSubmitted, "curr_subject_uuid", "curr-sub-1")
          assert[String](feedbackSubmitted, "fle_ls_uuid", "ls-id-1")
          assert[String](feedbackSubmitted, "lesson_feedback_trimester_id", "trimester-id-1")
          assert[Int](feedbackSubmitted, "lesson_feedback_trimester_order", 1)
          assert[String](feedbackSubmitted, "lesson_feedback_content_academic_year", "2020")
          assert[Int](feedbackSubmitted, "lesson_feedback_rating", 3)
          assert[String](feedbackSubmitted, "lesson_feedback_rating_text", "Neutral")
          assert[Boolean](feedbackSubmitted, "lesson_feedback_has_comment", true)
          assert[String](feedbackSubmitted, "lesson_feedback_instructional_plan_id", "ip1")
          assert[String](feedbackSubmitted, "lesson_feedback_learning_path_id", "lp1")
          assert[String](feedbackSubmitted, "lesson_feedback_teaching_period_id", "teaching-period-id-1")
          assert[String](feedbackSubmitted, "lesson_feedback_teaching_period_title", "teaching-period-title-1")

          val feedbackCancelled = df.filter("lesson_feedback_is_cancelled = true")
          assert[String](feedbackCancelled, "lesson_feedback_id", "feedback-id-2")
          assert[Int](feedbackCancelled, "lesson_feedback_rating", -1)
          assert[String](feedbackCancelled, "lesson_feedback_rating_text", "n/a")
          assert[Boolean](feedbackCancelled, "lesson_feedback_has_comment", false)
          assert[String](feedbackCancelled, "lesson_feedback_instructional_plan_id", null)
          assert[String](feedbackCancelled, "lesson_feedback_learning_path_id", null)

          val deltaSink = sinks.filter(_.name == DeltaLessonFeedbackSink).collectFirst { case s: DeltaCreateSink => s }.get
          val deltaSinkDF = deltaSink.output
          deltaSinkDF.columns.toSet must contain allElementsOf expectedColumns.map(_.replace("_uuid", "_id"))
          val expectedFields = List(
            ExpectedField(name = "lesson_feedback_comment", dataType = StringType),
            ExpectedField(name = "eventdate", dataType = StringType)
          )
          assertExpectedFields(deltaSinkDF.schema.fields.filter(f => f.name == "lesson_feedback_comment" || f.name == "eventdate").toList,
            expectedFields)
          val comment = deltaSinkDF.first.getAs[String]("lesson_feedback_comment")
          comment must not be empty
          assert[String](deltaSinkDF, "lesson_feedback_comment", "Good")
        }
      )
    }
  }

}
