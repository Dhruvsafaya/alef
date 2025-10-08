package com.alefeducation.facts

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.DataFrame

class AwardResourceAggregationSpec extends SparkSuite with BaseDimensionSpec {

  trait Setup {
    implicit val transformer = new AwardResourceAggregationTransformer(AwardResourceAggregationService, spark)
  }

  val expectedColumns = Set(
    "fsa_id",
    "award_category_uuid",
    "tenant_uuid",
    "school_uuid",
    "class_uuid",
    "subject_uuid",
    "grade_uuid",
    "student_uuid",
    "teacher_uuid",
    "fsa_award_comments",
    "fsa_date_dw_id",
    "fsa_created_time",
    "fsa_dw_created_time",
    "academic_year_uuid",
    "fsa_stars"
  )

  test("transform award stars event") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetAwardResourceSource,
          value = """
              |{
              |  "tenantId": "tenant-id",
              |  "eventType": "AwardResource",
              |  "id": "award-id",
              |  "schoolId": "school-id",
              |  "grade": 7,
              |  "gradeId": "grade-id",
              |  "section": "class-id",
              |  "trimesterId": "trimester-id",
              |  "classId": "class-id",
              |  "subjectId": "subject-id",
              |  "learnerId": "student-id",
              |  "teacherId": "teacher-id",
              |  "categoryCode": "gbh",
              |  "categoryLabelEn": "Good Behavior",
              |  "categoryLabelAr": "سلوك وخلق جيدين",
              |  "comment": "hey you",
              |  "createdOn": "2018-09-08T04:09:10.861",
              |  "eventDateDw": 20180908,
              |  "occurredOn": "2018-09-08 02:40:00.0",
              |  "academicYearId": "239b7b51-2377-4ea1-8aa2-a0aaad8e1a03",
              |  "stars": 3
              |}
      """.stripMargin
        )
      )
      withSparkFixturesWithSink(
        fixtures,
        RedshiftStarAwardSink, { df =>
          assert(df.columns.toSet === expectedColumns)
          assert[String](df, "fsa_id", "award-id")
          assert[String](df, "award_category_uuid", "gbh")
          assert[Boolean](df, "fsa_award_comments", true)
          assert[String](df, "grade_uuid", "grade-id")
          assert[String](df, "academic_year_uuid", "239b7b51-2377-4ea1-8aa2-a0aaad8e1a03")
          assert[Int](df, "fsa_stars", 3)

        }
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val filteredSinks = sinks.filter(_.name == DeltaStarAwardSink)
          val df: DataFrame = filteredSinks.head.output
          assert[String](df, "fsa_award_comments", "hey you")
          assert[Int](df, "fsa_stars", 3)

          val expectedColDelta = (expectedColumns.toSeq :+ "eventdate")
            .map(c => if (c.contains("uuid")) "fsa_" + c.replace("uuid", "id") else c)
          testSinkBySinkName(sinks, RedshiftStarAwardSink, expectedColumns.toSeq, 1)
          testSinkBySinkName(sinks, DeltaStarAwardSink, expectedColDelta.toSeq, 1)
        }
      )

    }
  }

}
