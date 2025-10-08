package com.alefeducation.dimensions

import com.alefeducation.bigdata.batch.delta.{DeltaUpdate, DeltaUpdateSink}
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.scalatest.matchers.must.Matchers

class ThemeDimensionSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer: ThemeDimension = ThemeDimension(spark)
  }

  val DeltaThemeMutatedExpectedFields = Set(
    "theme_id",
    "theme_name",
    "theme_description",
    "theme_curriculum_id",
    "theme_curriculum_grade_id",
    "theme_curriculum_subject_id",
    "theme_thumbnail_file_name",
    "theme_thumbnail_content_type",
    "theme_thumbnail_file_size",
    "theme_thumbnail_location",
    "theme_thumbnail_updated_at",
    "theme_created_at",
    "theme_updated_at",
    "theme_status"
  ) ++ dimDateCols("theme").toSet -- Set("theme_active_until")

  val DeltaThemeDeletedExpectedFields = Set(
    "theme_id",
    "theme_status"
  ) ++ dimDateCols("theme").toSet -- Set("theme_active_until")

  val RedshiftThemeMutatedExpectedFields = DeltaThemeMutatedExpectedFields -- Set(
    "theme_thumbnail_file_name",
    "theme_thumbnail_content_type",
    "theme_thumbnail_file_size",
    "theme_thumbnail_location",
    "theme_thumbnail_updated_at",
    "theme_description"
  )
  val RedshiftThemeDeletedExpectedFields = DeltaThemeDeletedExpectedFields

  test("ThemeCreatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "parquet-theme-created",
          value = """
              |[
              |{
              |    "eventType": "ThemeCreatedEvent",
              |    "id": 21,
              |    "name": "DemoTheme",
              |    "description": "DemoTheme Desc",
              |    "boardId": 392027,
              |    "gradeId": 322135,
              |    "subjectId": 571671,
              |    "thumbnailFileName": "21_thumb.png",
              |    "thumbnailContentType": "image/png",
              |    "thumbnailFileSize": 344022,
              |    "thumbnailLocation": "3c/59/dc/21",
              |    "thumbnailUpdatedAt": 1586342333335,
              |    "createdAt": 1586342333335,
              |    "eventDateDw": "20200708",
              |    "occurredOn": "08-04-2020 10:38:55.109",
              |    "loadtime": "2020-07-08T05:18:02.053Z"
              |}
              |]
              |""".stripMargin
        ),
        SparkFixture(
          key = "parquet-theme-updated",
          value = """
                    |[{
                    |    "eventType": "ThemeUpdatedEvent",
                    |    "id": 21,
                    |    "name": "DemoThemeChanged",
                    |    "description": "DemoTheme Desc",
                    |    "boardId": 392027,
                    |    "gradeId": 322135,
                    |    "subjectId": 571671,
                    |    "thumbnailFileName": "21_thumb.png",
                    |    "thumbnailContentType": "image/png",
                    |    "thumbnailFileSize": 344022,
                    |    "thumbnailLocation": "3c/59/dc/21",
                    |    "thumbnailUpdatedAt": 1586342333335,
                    |    "updatedAt": 1586342333335,
                    |    "eventDateDw": "20200708",
                    |    "occurredOn": "08-04-2020 10:38:55.109",
                    |    "loadtime": "2020-07-08T05:18:02.053Z"
                    |}]
                    |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val deltaCreatedSink = sinks.find { s => s.name == "delta-theme-sink" && s.eventType == "ThemeCreatedEvent"
          }.get
          assert(deltaCreatedSink.output.columns.toSet === DeltaThemeMutatedExpectedFields)
          assert[Int](deltaCreatedSink.output, "theme_status", 1)

          val deltaUpdatedSink = sinks.collectFirst { case s: DeltaUpdateSink if s.name == "delta-theme-sink" => s }.get
          deltaUpdatedSink.updateFields.keySet mustNot contain("theme_created_at")

          val redshiftCreatedSink = sinks.find { s => s.name == "redshift-theme-sink" && s.eventType == "ThemeCreatedEvent"
          }.get
          assert(redshiftCreatedSink.output.columns.toSet === RedshiftThemeMutatedExpectedFields)
          assert[Int](redshiftCreatedSink.output, "theme_status", 1)

          val parquetMutatedSinks = sinks.filter(_.name == "parquet-theme-created")
          val parquetCreatedDf = parquetMutatedSinks.head.output.where("eventType = 'ThemeCreatedEvent'")
          assert(!parquetCreatedDf.isEmpty)
        }
      )
    }
  }

  test("ThemeUpdatedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "parquet-theme-updated",
          value = """
                    |[
                    |{
                    |    "eventType": "ThemeUpdatedEvent",
                    |    "id": 21,
                    |    "name": "DemoTheme",
                    |    "description": "DemoTheme Desc",
                    |    "boardId": 392027,
                    |    "gradeId": 322135,
                    |    "subjectId": 571671,
                    |    "thumbnailFileName": "21_thumb.png",
                    |    "thumbnailContentType": "image/png",
                    |    "thumbnailFileSize": 344022,
                    |    "thumbnailLocation": "3c/59/dc/21",
                    |    "thumbnailUpdatedAt": 1586342333335,
                    |    "updatedAt": 1586342333335,
                    |    "eventDateDw": "20200708",
                    |    "occurredOn": "08-04-2020 10:38:55.109",
                    |    "loadtime": "2020-07-08T05:18:02.053Z"
                    |}
                    |]
                    |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          sinks.find { s => s.name == "delta-theme-sink" && s.eventType == "ThemeCreatedEvent"
          } mustBe empty
          val deltaUpdatedSink = sinks.find { s => s.name == "delta-theme-sink" && s.eventType == "ThemeUpdatedEvent"
          }.get
          assert(deltaUpdatedSink.output.columns.toSet === DeltaThemeMutatedExpectedFields)
          assert[Int](deltaUpdatedSink.output, "theme_status", 1)

          val redshiftUpdatedSink = sinks.find { s => s.name == "redshift-theme-sink" && s.eventType == "ThemeUpdatedEvent"
          }.get
          assert(redshiftUpdatedSink.output.columns.toSet === RedshiftThemeMutatedExpectedFields)
          assert[Int](redshiftUpdatedSink.output, "theme_status", 1)

          val parquetMutatedSinks = sinks.filter(_.name == "parquet-theme-updated")
          val parquetUpdatedDf = parquetMutatedSinks.head.output.where("eventType = 'ThemeUpdatedEvent'")
          assert(!parquetUpdatedDf.isEmpty)

        }
      )
    }
  }

  test("ThemeDeletedEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "parquet-theme-deleted",
          value = """
                    |[
                    |  {
                    |    "eventType": "ThemeDeletedEvent",
                    |    "loadtime": "2020-07-08T05:18:02.053Z",
                    |    "id": 21,
                    |    "occurredOn": "2020-07-08 05:18:02.030",
                    |    "eventDateDw": "20200708"
                    |  }
                    |]
                    |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val deltaDeletedSink = sinks.find { s => s.name == "delta-theme-sink" && s.eventType == "ThemeDeletedEvent"
          }.get
          assert(deltaDeletedSink.output.columns.toSet === DeltaThemeDeletedExpectedFields)
          assert[Int](deltaDeletedSink.output, "theme_status", 4)

          val redshiftDeletedSink = sinks.find { s => s.name == "redshift-theme-sink" && s.eventType == "ThemeDeletedEvent"
          }.get
          assert(redshiftDeletedSink.output.columns.toSet === RedshiftThemeDeletedExpectedFields)
          assert[Int](redshiftDeletedSink.output, "theme_status", 4)

          val parquetDeletedDf = sinks.find(_.name == "parquet-theme-deleted").get.output
          assert(!parquetDeletedDf.isEmpty)
        }
      )
    }
  }

}
