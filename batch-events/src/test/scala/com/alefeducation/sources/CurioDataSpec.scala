package com.alefeducation.sources

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers._

class CurioDataSpec extends SparkSuite {

  trait Setup {
    implicit lazy val transformer: CurioDataTransformer = new CurioDataTransformer("curio", spark)
  }

  val expectedStudentParquetColumns =
    Set("adek_id", "timestamp", "status", "interaction_type", "topic_name", "subject_id", "eventDateDw", "eventdate", "loadtime")
  val expectedTeacherParquetColumns = Set("student", "teacher", "topic", "createdat", "updatedat", "eventDateDw", "eventdate", "loadtime")
  val expectedStudentColumns =
    Set("adek_id", "time_stamp", "status", "interaction_type", "topic_name", "subject_id", "role", "date_dw_id", "loadtime")
  val expectedTeacherColumns = Set("student_id", "teacher_id", "topic", "created_at", "updated_at", "date_dw_id", "loadtime")

  test("if empty DataFrame comes") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "csv-curio-student-source",
          value = ""
        )
      )
      withSparkFixtures(fixtures, sinks => assert(sinks.isEmpty))
    }
  }

  test("curio student for success case") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "csv-curio-student-source",
          value = """
              |adek_id,timestamp,status,interaction_type,topic_name,subject_id,role
              |adek-id-1,2019-09-10 04:57:41.052000,succeed,play,Add Whole Numbers,math,student
            """.stripMargin
        ),
        SparkFixture(
          key = RedshiftDimStudentSink,
          value = """
                    |[{
                    |   "student_status":1,
                    |   "student_dw_id":1,
                    |   "student_id":"student-id-dim",
                    |   "student_created_time": "2019-03-09 02:30:00.0",
                    |   "student_username":"student-username",
                    |   "student_school_dw_id" : "1",
                    |   "student_grade_dw_id":"1",
                    |   "student_class_dw_id":"1",
                    |   "student_tags":"ELITE",
                    |   "student_active_until": null
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 2)
          val curioStudentDf = sinks.filter(_.name.contains("student"))
          val s3Sink = curioStudentDf.head
          val redshiftSink = curioStudentDf.last.output

          assert(redshiftSink.count === 1)
          assert(redshiftSink.columns.toSet === expectedStudentColumns)
          assert[String](redshiftSink, "adek_id", "adek-id-1")

          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3Sink.output, "eventdate", "2019-09-10")
        }
      )
    }
  }

  test("curio teacher for success case") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "csv-curio-teacher-source",
          value = """
              |student,teacher,topic,createdat,updatedat
              |student-id-1,teacher-id-1,topic,2019-09-02 04:29:11.984,2019-09-02 04:29:11.984
            """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 2)
          val curioStudentDf = sinks.filter(_.name.contains("teacher"))
          val s3Sink = curioStudentDf.head
          val redshiftSink = curioStudentDf.last.output

          assert(redshiftSink.count === 1)
          assert(redshiftSink.columns.toSet === expectedTeacherColumns)
          assert[String](redshiftSink, "teacher_id", "teacher-id-1")

          assert(s3Sink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert(s3Sink.output.columns.toSet === expectedTeacherParquetColumns)
          assert[String](s3Sink.output, "eventdate", "2019-09-02")
        }
      )
    }
  }

  test("curio teacher and student success case") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "csv-curio-student-source",
          value = """
              |adek_id,timestamp,status,interaction_type,topic_name,subject_id,role
              |adek-id-1,2019-09-10 04:57:41.052000,succeed,play,Add Whole Numbers,math,student
            """.stripMargin
        ),
        SparkFixture(
          key = "csv-curio-teacher-source",
          value = """
              |student,teacher,topic,createdat,updatedat
              |student-id-1,teacher-id-1,topic,2019-09-02 04:29:11.984,2019-09-02 04:29:11.984
            """.stripMargin
        ),
        SparkFixture(
          key = RedshiftDimStudentSink,
          value = """
                    |[{
                    |   "student_status":1,
                    |   "student_dw_id":1,
                    |   "student_id":"student-id-1",
                    |   "student_created_time": "2019-03-09 02:30:00.0",
                    |   "student_username":"adek-id-1",
                    |   "student_school_dw_id" : "1",
                    |   "student_grade_dw_id":"1",
                    |   "student_class_dw_id":"1",
                    |   "student_tags":"ELITE",
                    |   "student_active_until": null
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 4)
          val curioStudentDf = sinks.filter(_.name.contains("student"))
          val s3StudentSink = curioStudentDf.head
          val redshiftStudentSink = curioStudentDf.last.output
          assert(redshiftStudentSink.count === 1)
          assert(redshiftStudentSink.columns.toSet === expectedStudentColumns)
          assert[String](redshiftStudentSink, "adek_id", "adek-id-1")
          assert(s3StudentSink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3StudentSink.output, "eventdate", "2019-09-10")

          val curioTeacherDf = sinks.filter(_.name.contains("teacher"))
          val s3TeacherSink = curioTeacherDf.head
          val redshiftTeacherSink = curioTeacherDf.last.output
          assert(redshiftTeacherSink.count === 1)
          assert(redshiftTeacherSink.columns.toSet === expectedTeacherColumns)
          assert[String](redshiftTeacherSink, "teacher_id", "teacher-id-1")
          assert(s3TeacherSink.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate"))
          assert[String](s3TeacherSink.output, "eventdate", "2019-09-02")
        }
      )
    }
  }
}
