package com.alefeducation.facts.learning_experience

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.learning_experience.redshift.ExperienceTotalScoreUpdatedRedshift
import com.alefeducation.facts.learning_experience.redshift.ExperienceTotalScoreUpdatedRedshift.LearningSessionRedshiftService
import com.alefeducation.service.SparkBatchService
import org.apache.spark.sql.SparkSession

class ExperienceTotalScoreUpdatedSpec extends SparkSuite {

  import com.alefeducation.bigdata.batch.delta.DeltaCreateSink
  import com.alefeducation.bigdata.commons.testutils.SparkSuite
  import com.alefeducation.schema.lps.ActivityComponent
  import com.alefeducation.util.Helpers._
  import org.scalatest.matchers.must.Matchers

  class ExperienceTotalScoreUpdatedSpec extends SparkSuite with Matchers {

    trait Setup {
      class ExperienceTotalScoreUpdated extends SparkBatchService {

        override def transform(): List[Sink] = {
          val service = new com.alefeducation.base.SparkBatchService(LearningSessionRedshiftService, session)
          new ExperienceTotalScoreUpdatedRedshift(spark, service).transform().toList
        }

        override val name: String = "test"
        override val session: SparkSession = spark
      }

      implicit val transformer = new ExperienceTotalScoreUpdated()
    }

    val expectedColumns: Set[String] = Set(
      "fle_date_dw_id",
      "fle_exp_id",
      "tenant_uuid",
      "curr_subject_uuid",
      "fle_end_time",
      "fle_score",
      "fle_instructional_plan_id",
      "fle_time_spent_app",
      "fle_activity_type",
      "academic_year_uuid",
      "curr_uuid",
      "fle_star_earned",
      "fle_start_time",
      "fle_is_retry",
      "school_uuid",
      "fle_academic_period_order",
      "lo_uuid",
      "curr_grade_uuid",
      "fle_total_score",
      "fle_total_stars",
      "grade_uuid",
      "fle_created_time",
      "lp_uuid",
      "fle_dw_created_time",
      "fle_content_academic_year",
      "student_uuid",
      "fle_activity_template_id",
      "fle_ls_id",
      "fle_outside_of_school",
      "fle_is_activity_completed",
      "fle_lesson_category",
      "section_uuid",
      "subject_uuid",
      "class_uuid",
      "fle_total_time",
      "fle_attempt",
      "fle_activity_component_type",
      "fle_completion_node",
      "fle_lesson_type",
      "fle_abbreviation",
      "fle_exit_ticket",
      "fle_main_component",
      "fle_material_id",
      "fle_material_type",
      "fle_state",
      "fle_open_path_enabled",
      "fle_teaching_period_id",
      "fle_academic_year",
    )

    test("transform total score update") {
      new Setup {
        val fixtures = List(
          SparkFixture(
            key = ParquetTotalScoreUpdatedSource,
            value = """
                  |{
                  |  "tenantId": "tenant-id",
                  |  "eventType": "TotalScoreUpdatedEvent",
                  |  "uuid":"bfba9302-da96-408f-a0b4-dfe80504a1fc",
                  |  "occurredOn":"2021-09-13T09:14:20.561",
                  |  "attempt":1,
                  |  "learningSessionId":"d105ac31-7731-435f-a935-3239baa9b152",
                  |  "learningObjectiveId":"726185bf-32d7-4991-9f6b-000000027571",
                  |  "studentId":"13dcc782-74dd-4d72-a144-f7f67ee84a9f",
                  |  "studentName":"St60 Test",
                  |  "studentK12Grade":7,
                  |  "studentGradeId":"f87b5a01-5327-40cf-ad96-2dddb5abdff5",
                  |  "studentSection":"960998a9-584d-4add-9557-cdeead3395ee",
                  |  "learningPathId":"4c172cc6-0c43-4cc4-9648-3e7756261482",
                  |  "instructionalPlanId":"f92c4dda-0ad9-464b-a005-67d581ada1ed",
                  |  "subjectId":null,
                  |  "subjectCode":"ENGLISH_MOE",
                  |  "subjectName":"English",
                  |  "learningObjectiveCode":"EN7_MLO_204_ASGN",
                  |  "learningObjectiveTitle":"Using Percentages and Fractions_A",
                  |  "learningObjectiveType":"FF4",
                  |  "schoolId":"fa465fed-41ac-4165-9849-e20426ba688d",
                  |  "trimesterId":"bd94be12-6c63-41d0-855c-80dbb2e01fd8",
                  |  "trimesterOrder":3,
                  |  "curriculumId":"392027",
                  |  "curriculumName":"UAE MOE",
                  |  "curriculumSubjectId":"423412",
                  |  "curriculumSubjectName":"English_MOE",
                  |  "curriculumGradeId":"596550",
                  |  "academicYearId":"78ef63bc-3156-4b31-b8e4-b48f03c0b6e1",
                  |  "contentAcademicYear":"2021",
                  |  "classId":"c4aa4997-7599-423a-9ef4-e6b3d3929e63",
                  |  "startTime":"2021-09-13T09:12:19.253",
                  |  "outsideOfSchool":false,
                  |  "practiceEnabled":false,
                  |  "lessonCategory":"INSTRUCTIONAL_LESSON",
                  |  "activityTemplateId":"FF4",
                  |  "activityType":"INSTRUCTIONAL_LESSON",
                  |  "activityComponents":[
                  |     {
                  |        "activityComponentType":"CONTENT",
                  |        "exitTicket":false,
                  |        "activityComponentId":"9e258568-9551-491b-9e77-09e0575da0e0",
                  |        "mainComponent":true,
                  |        "alwaysEnabled":true,
                  |        "abbreviation":"AC"
                  |     },
                  |     {
                  |        "activityComponentType":"CONTENT",
                  |        "exitTicket":false,
                  |        "activityComponentId":"68a60a05-d9d6-4dab-bb1e-0bafa1ba6a4c",
                  |        "mainComponent":false,
                  |        "alwaysEnabled":false,
                  |        "abbreviation":"MP"
                  |     },
                  |     {
                  |        "activityComponentType":"KEY_TERM",
                  |        "exitTicket":false,
                  |        "activityComponentId":"e8c056db-f5bd-49a3-a59d-f28aee712541",
                  |        "mainComponent":false,
                  |        "alwaysEnabled":false,
                  |        "abbreviation":"KT"
                  |     },
                  |     {
                  |        "activityComponentType":"CONTENT",
                  |        "exitTicket":false,
                  |        "activityComponentId":"b04817fd-7633-41f7-ab9c-eff891e982fa",
                  |        "mainComponent":true,
                  |        "alwaysEnabled":true,
                  |        "abbreviation":"LESSON_1"
                  |     },
                  |     {
                  |        "activityComponentType":"ASSESSMENT",
                  |        "exitTicket":false,
                  |        "activityComponentId":"c81c247d-266f-48e4-937d-c24c5d6582a3",
                  |        "mainComponent":true,
                  |        "alwaysEnabled":true,
                  |        "abbreviation":"TEQ_1"
                  |     },
                  |     {
                  |        "activityComponentType":"CONTENT",
                  |        "exitTicket":false,
                  |        "activityComponentId":"85f42ee6-a4a0-4dd2-979c-6e5c5b955f31",
                  |        "mainComponent":false,
                  |        "alwaysEnabled":false,
                  |        "abbreviation":"R"
                  |     },
                  |     {
                  |        "activityComponentType":"CONTENT",
                  |        "exitTicket":false,
                  |        "activityComponentId":"f0c8afec-f250-4a44-b2be-c98fee13b2f0",
                  |        "mainComponent":true,
                  |        "alwaysEnabled":true,
                  |        "abbreviation":"LESSON_2"
                  |     },
                  |     {
                  |        "activityComponentType":"ASSESSMENT",
                  |        "exitTicket":false,
                  |        "activityComponentId":"963fb97e-73ee-4929-a113-b374339ece13",
                  |        "mainComponent":true,
                  |        "alwaysEnabled":true,
                  |        "abbreviation":"TEQ_2"
                  |     },
                  |     {
                  |        "activityComponentType":"ASSESSMENT",
                  |        "exitTicket":true,
                  |        "activityComponentId":"e586a18c-7008-4c27-8846-6282e05e7f31",
                  |        "mainComponent":true,
                  |        "alwaysEnabled":true,
                  |        "abbreviation":"SA"
                  |     },
                  |     {
                  |        "activityComponentType":"ASSIGNMENT",
                  |        "exitTicket":false,
                  |        "activityComponentId":"0e03e0d8-7204-40bf-90bf-0455b7fbe0de",
                  |        "mainComponent":true,
                  |        "alwaysEnabled":true,
                  |        "abbreviation":"ASGN"
                  |     },
                  |     {
                  |        "activityComponentType":"CONTENT",
                  |        "exitTicket":false,
                  |        "activityComponentId":"974ab4f0-6ae4-48f4-93f8-84823640c670",
                  |        "mainComponent":false,
                  |        "alwaysEnabled":false,
                  |        "abbreviation":"R_2"
                  |     }
                  |  ],
                  |  "stars":1,
                  |  "score":0,
                  |  "timeSpent":79,
                  |  "totalScore":37.5,
                  |  "activityCompleted":false,
                  |  "eventRoute":"learning.score.updated",
                  |  "eventSemanticId":"d105ac31-7731-435f-a935-3239baa9b152",
                  |  "studentGrade":"7",
                  |  "redo":false,
                  |  "eventDateDw": "20210913",
                  |  "materialId": "e2739972-3e4e-4dfe-bcd4-82aaf121f12f",
                  |  "materialType": "INSTRUCTIONAL_PLAN",
                  |  "academicYear": "2024",
                  |  "teachingPeriodId": "period_id1",
                  |  "replayed": false
                  |}
        """.stripMargin
          )
        )

        withSparkFixtures(
          fixtures, { sinks =>
            val redshiftSink = sinks.find(_.name == RedshiftLearningExperienceSink).get
            val redshiftSinkDF = redshiftSink.output
            assert(redshiftSinkDF.columns.toSet === expectedColumns)
            assert[String](redshiftSinkDF, "fle_exp_id", "bfba9302-da96-408f-a0b4-dfe80504a1fc")
            assert[String](redshiftSinkDF, "fle_ls_id", "d105ac31-7731-435f-a935-3239baa9b152")
            assert[String](redshiftSinkDF, "lo_uuid", "726185bf-32d7-4991-9f6b-000000027571")
            assert[String](redshiftSinkDF, "student_uuid", "13dcc782-74dd-4d72-a144-f7f67ee84a9f")
            assert[String](redshiftSinkDF, "fle_activity_template_id", "FF4")
            assert[String](redshiftSinkDF, "subject_uuid", null)
            assert[String](redshiftSinkDF, "section_uuid", "960998a9-584d-4add-9557-cdeead3395ee")
            assert[String](redshiftSinkDF, "class_uuid", "c4aa4997-7599-423a-9ef4-e6b3d3929e63")
            assert[Int](redshiftSinkDF, "fle_academic_period_order", 3)
            assert[String](redshiftSinkDF, "curr_uuid", "392027")
            assert[String](redshiftSinkDF, "curr_subject_uuid", "423412")
            assert[String](redshiftSinkDF, "curr_grade_uuid", "596550")
            assert[Boolean](redshiftSinkDF, "fle_is_retry", false)
            assert[String](redshiftSinkDF, "academic_year_uuid", "78ef63bc-3156-4b31-b8e4-b48f03c0b6e1")
            assert[String](redshiftSinkDF, "fle_content_academic_year", "2021")
            assert[Int](redshiftSinkDF, "fle_time_spent_app", 79)
            assert[String](redshiftSinkDF, "fle_instructional_plan_id", "f92c4dda-0ad9-464b-a005-67d581ada1ed")
            assert[String](redshiftSinkDF, "fle_lesson_category", "INSTRUCTIONAL_LESSON")
            assert[String](redshiftSinkDF, "fle_date_dw_id", "20210913")
            assert[Double](redshiftSinkDF, "fle_total_score", 37.5)
            assert[Int](redshiftSinkDF, "fle_total_stars", 1)
            assert[Boolean](redshiftSinkDF, "fle_is_activity_completed", false)
            assert[String](redshiftSinkDF, "fle_activity_type", "INSTRUCTIONAL_LESSON")
            assert[Int](redshiftSinkDF, "fle_star_earned", 1)
            assert[String](redshiftSinkDF, "fle_start_time", "2021-09-13 09:12:19.253")
            assert[String](redshiftSinkDF, "school_uuid", "fa465fed-41ac-4165-9849-e20426ba688d")
            assert[String](redshiftSinkDF, "fle_created_time", "2021-09-13 09:14:20.561")
            assert[String](redshiftSinkDF, "lp_uuid", "4c172cc6-0c43-4cc4-9648-3e7756261482")
            assert[Boolean](redshiftSinkDF, "fle_outside_of_school", false)
            assert[String](redshiftSinkDF, "tenant_uuid", "tenant-id")
            assert[Double](redshiftSinkDF, "fle_score", 0)
            assert[String](redshiftSinkDF, "fle_activity_component_type", "NA")
            assert[Boolean](redshiftSinkDF, "fle_completion_node", false)
            assert[String](redshiftSinkDF, "fle_lesson_type", "Unlock")
            assert[String](redshiftSinkDF, "fle_abbreviation", "NA")
            assert[Boolean](redshiftSinkDF, "fle_exit_ticket", false)
            assert[Boolean](redshiftSinkDF, "fle_exit_ticket", false)
            assert[Boolean](redshiftSinkDF, "fle_main_component", false)
            assert[String](redshiftSinkDF, "fle_material_id", "e2739972-3e4e-4dfe-bcd4-82aaf121f12f")
            assert[String](redshiftSinkDF, "fle_material_type", "INSTRUCTIONAL_PLAN")
            assert[Int](redshiftSinkDF, "fle_state", 3)
            assert[String](redshiftSinkDF, "fle_academic_year", "2024")
            assert[String](redshiftSinkDF, "fle_teaching_period_id", "period_id1")

            val deltaSink = sinks.collectFirst { case s: DeltaCreateSink => s }.get
            val deltaSinkDF = deltaSink.output
            deltaSinkDF.columns.toSet must contain allElementsOf expectedColumns
            assert(deltaSinkDF.columns.contains("fle_activity_components"))
            val activityComponents = deltaSinkDF.first.getAs[Seq[ActivityComponent]]("fle_activity_components")
            activityComponents must not be empty
            assert[String](deltaSinkDF, "fle_exp_id", "bfba9302-da96-408f-a0b4-dfe80504a1fc")
            assert[String](deltaSinkDF, "fle_ls_id", "d105ac31-7731-435f-a935-3239baa9b152")
            assert[String](deltaSinkDF, "lo_uuid", "726185bf-32d7-4991-9f6b-000000027571")
            assert[String](deltaSinkDF, "student_uuid", "13dcc782-74dd-4d72-a144-f7f67ee84a9f")
            assert[String](deltaSinkDF, "fle_activity_template_id", "FF4")
            assert[String](deltaSinkDF, "subject_uuid", null)
            assert[String](deltaSinkDF, "section_uuid", "960998a9-584d-4add-9557-cdeead3395ee")
            assert[String](deltaSinkDF, "class_uuid", "c4aa4997-7599-423a-9ef4-e6b3d3929e63")
            assert[Int](deltaSinkDF, "fle_academic_period_order", 3)
            assert[String](deltaSinkDF, "curr_uuid", "392027")
            assert[String](deltaSinkDF, "curr_subject_uuid", "423412")
            assert[String](deltaSinkDF, "curr_grade_uuid", "596550")
            assert[Boolean](deltaSinkDF, "fle_is_retry", false)
            assert[String](deltaSinkDF, "academic_year_uuid", "78ef63bc-3156-4b31-b8e4-b48f03c0b6e1")
            assert[String](deltaSinkDF, "fle_content_academic_year", "2021")
            assert[Int](deltaSinkDF, "fle_time_spent_app", 79)
            assert[String](deltaSinkDF, "fle_instructional_plan_id", "f92c4dda-0ad9-464b-a005-67d581ada1ed")
            assert[String](deltaSinkDF, "fle_lesson_category", "INSTRUCTIONAL_LESSON")
            assert[String](deltaSinkDF, "fle_date_dw_id", "20210913")
            assert[Double](deltaSinkDF, "fle_total_score", 37.5)
            assert[Int](deltaSinkDF, "fle_total_stars", 1)
            assert[Boolean](deltaSinkDF, "fle_is_activity_completed", false)
            assert[String](deltaSinkDF, "fle_activity_type", "INSTRUCTIONAL_LESSON")
            assert[Int](deltaSinkDF, "fle_star_earned", 1)
            assert[String](deltaSinkDF, "fle_start_time", "2021-09-13 09:12:19.253")
            assert[String](deltaSinkDF, "school_uuid", "fa465fed-41ac-4165-9849-e20426ba688d")
            assert[String](deltaSinkDF, "fle_created_time", "2021-09-13 09:14:20.561")
            assert[String](deltaSinkDF, "lp_uuid", "4c172cc6-0c43-4cc4-9648-3e7756261482")
            assert[Boolean](deltaSinkDF, "fle_outside_of_school", false)
            assert[String](deltaSinkDF, "tenant_uuid", "tenant-id")
            assert[Double](deltaSinkDF, "fle_score", 0)
            assert[String](deltaSinkDF, "fle_activity_component_type", "NA")
            assert[Boolean](deltaSinkDF, "fle_completion_node", false)
            assert[String](deltaSinkDF, "fle_lesson_type", "Unlock")
            assert[String](deltaSinkDF, "fle_abbreviation", "NA")
            assert[Boolean](deltaSinkDF, "fle_exit_ticket", false)
            assert[Boolean](deltaSinkDF, "fle_exit_ticket", false)
            assert[Boolean](deltaSinkDF, "fle_main_component", false)
            assert[String](deltaSinkDF, "fle_material_id", "e2739972-3e4e-4dfe-bcd4-82aaf121f12f")
            assert[String](deltaSinkDF, "fle_material_type", "INSTRUCTIONAL_PLAN")
            assert[Int](deltaSinkDF, "fle_state", 3)
            assert[String](deltaSinkDF, "fle_academic_year", "2024")
            assert[String](deltaSinkDF, "fle_teaching_period_id", "period_id1")
          }
        )
      }
    }
  }

}
