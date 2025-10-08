package com.alefeducation.facts

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers.{DeltaStarAwardSink, RedshiftStarAwardSink}
import com.alefeducation.util.ktgame.KTGameHelper._

class KTGameSpec extends SparkSuite with BaseDimensionSpec {

  trait Setup {
    implicit val transformer = new KTGameTransformer(ktGameService, spark)
  }

  val ktGameCreatedColumns = Set(
    "ktg_id",
    "ktg_created_time",
    "ktg_dw_created_time",
    "ktg_date_dw_id",
    "tenant_uuid",
    "student_uuid",
    "subject_uuid",
    "school_uuid",
    "grade_uuid",
    "section_uuid",
    "lo_uuid",
    "ktg_num_key_terms",
    "ktg_kt_collection_id",
    "ktg_trimester_id",
    "ktg_trimester_order",
    "ktg_type",
    "ktg_question_type",
    "ktg_min_question",
    "ktg_max_question",
    "ktg_question_time_allotted",
    "academic_year_uuid",
    "ktg_instructional_plan_id",
    "ktg_learning_path_id",
    "class_uuid",
    "ktg_material_id",
    "ktg_material_type"
  )

  test("transform KT game created events successfully") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetKTGameSource,
          value = """
              |{
              |  "tenantId": "tenantId",
              |  "eventType": "GameSessionCreatedEvent",
              |  "occurredOn": "2019-06-25 08:33:26.585",
              |  "created": "2019-06-25T08:33:26.583",
              |  "gameSessionId": "game-session-1",
              |  "learnerId": "student-1",
              |  "subjectId": "subject-1",
              |  "subjectName": "Math",
              |  "subjectCode": "MATH",
              |  "consideredLos":
              |    [
              |      {
              |          "id": "lo-1",
              |          "code": "MA6_MLO_RN_010",
              |          "title": "Rational Numbers 4KT",
              |          "framework": "FF4",
              |          "thumbnail": "/content/data/ccl/mlo/thumbnails/05/1e/4e/629/MA6_MLO_RN_010_thumb.png",
              |          "ktCollectionId": "629",
              |          "numberOfKeyTerms": 4
              |      },
              |      {
              |          "id": "lo-1",
              |          "code": "MA6_MLO_RN_010",
              |          "title": "Rational Numbers 4KT",
              |          "framework": "FF4",
              |          "thumbnail": "/content/data/ccl/mlo/thumbnails/05/1e/4e/629/MA6_MLO_RN_010_thumb.png",
              |          "ktCollectionId": "629",
              |          "numberOfKeyTerms": 4
              |      },
              |      {
              |          "id": "lo-2",
              |          "code": "MA6_NT_MLO_01",
              |          "title": "Number Theory 12KT",
              |          "framework": "FF4",
              |          "thumbnail": "/content/data/ccl/mlo/thumbnails/9c/c1/38/630/MA6_NT_MLO_01_thumb.jpeg",
              |          "ktCollectionId": "630",
              |          "numberOfKeyTerms": 12
              |      }
              |    ],
              |  "gameConfig": {
              |    "type": "ROCKET",
              |    "question": {
              |      "type": "MULTIPLE_CHOICE",
              |      "time": "8",
              |      "min": "6",
              |      "max": "30"
              |    }
              |  },
              |  "trimesterId": "trimester-1",
              |  "trimesterOrder": 3,
              |  "schoolId": "school-1",
              |  "gradeId": "grade-1",
              |  "grade": 6,
              |  "sectionId": "section-1",
              |  "gameType": "ROCKET",
              |  "loadtime": "gIx6AgUcAAAkhCUA",
              |  "eventDateDw": "20190625",
              |  "academicYearId": "763578c8-a546-4c74-b660-5df866a337c9",
              |  "instructionalPlanId":"ip1",
              |  "learningPathId":"lp1",
              |  "classId": "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312",
              |  "materialId": "ktd1b4e4-9eb5-4eff-92a4-94139f892300",
              |  "materialType": "CORE"
              |}
      """.stripMargin
        ),
        SparkFixture(
          key = ParquetKTGameSkippedSource,
          value = """
              |{
              |  "tenantId": "tenantId",
              |  "eventType": "GameSessionCreationSkippedEvent",
              |  "occurredOn": "2019-06-25 08:33:26.585",
              |  "learnerId": "student-1",
              |  "subjectId": "subject-1",
              |  "subjectName": "Math",
              |  "subjectCode": "MATH",
              |  "consideredLos":
              |     [
              |      {
              |          "id": "lo-1",
              |          "code": "MA6_MLO_RN_010",
              |          "title": "Rational Numbers 4KT",
              |          "framework": "FF4",
              |          "thumbnail": "/content/data/ccl/mlo/thumbnails/05/1e/4e/629/MA6_MLO_RN_010_thumb.png",
              |          "ktCollectionId": "629",
              |          "numberOfKeyTerms": 4
              |      },
              |      {
              |          "id": "lo-2",
              |          "code": "MA6_NT_MLO_01",
              |          "title": "Number Theory 12KT",
              |          "framework": "FF4",
              |          "thumbnail": "/content/data/ccl/mlo/thumbnails/9c/c1/38/630/MA6_NT_MLO_01_thumb.jpeg",
              |          "ktCollectionId": null,
              |          "numberOfKeyTerms": null
              |      },
              |      {
              |          "id": "lo-3",
              |          "code": "MA6_SP_MLO_01",
              |          "title": "Scalar Product 0KT",
              |          "framework": "FF4",
              |          "thumbnail": "",
              |          "ktCollectionId": null,
              |          "numberOfKeyTerms": 3
              |      }
              |    ],
              |  "gameConfig": {
              |    "type": "ROCKET",
              |    "question": {
              |      "type": "MULTIPLE_CHOICE",
              |      "time": "8",
              |      "min": "6",
              |      "max": "30"
              |    }
              |  },
              |  "trimesterId": "trimester-1",
              |  "trimesterOrder": 3,
              |  "schoolId": "school-1",
              |  "gradeId": "grade-1",
              |  "grade": 6,
              |  "sectionId": "section-1",
              |  "loadtime": "gIx6AgUcAAAkhCUA",
              |  "eventDateDw": "20190625",
              |  "academicYearId": "763578c8-a546-4c74-b660-5df866a337c9",
              |  "instructionalPlanId":"ip1",
              |  "learningPathId":"lp1",
              |  "classId": "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312",
              |  "materialId": "ktd1b4e4-9eb5-4eff-92a4-94139f892300",
              |  "materialType": "CORE"
              |}
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          assert(sinks.size == 6)

          val gameCreatedDf = sinks.filter(_.name == RedshiftKTGameSink).head.output
          assert(gameCreatedDf.count === 2)

          val gameCreatedLO = gameCreatedDf.filter("lo_uuid = 'lo-1'")
          assert[String](gameCreatedLO, "tenant_uuid", "tenantId")
          assert[String](gameCreatedLO, "student_uuid", "student-1")
          assert[String](gameCreatedLO, "subject_uuid", "subject-1")
          assert[String](gameCreatedLO, "school_uuid", "school-1")
          assert[String](gameCreatedLO, "grade_uuid", "grade-1")
          assert[String](gameCreatedLO, "section_uuid", "section-1")
          assert[Int](gameCreatedLO, "ktg_num_key_terms", 4)
          assert[Long](gameCreatedLO, "ktg_kt_collection_id", 629)
          assert[String](gameCreatedLO, "ktg_trimester_id", "trimester-1")
          assert[Int](gameCreatedLO, "ktg_trimester_order", 3)
          assert[String](gameCreatedLO, "ktg_question_type", "MULTIPLE_CHOICE")
          assert[Int](gameCreatedLO, "ktg_min_question", 6)
          assert[Int](gameCreatedLO, "ktg_max_question", 30)
          assert[Int](gameCreatedLO, "ktg_question_time_allotted", 8)
          assert[String](gameCreatedLO, "ktg_id", "game-session-1")
          assert[String](gameCreatedLO, "academic_year_uuid", "763578c8-a546-4c74-b660-5df866a337c9")
          assert[String](gameCreatedLO, "ktg_instructional_plan_id", "ip1")
          assert[String](gameCreatedLO, "class_uuid", "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312")
          assert[String](gameCreatedLO, "ktg_material_id", "ktd1b4e4-9eb5-4eff-92a4-94139f892300")
          assert[String](gameCreatedLO, "ktg_material_type", "CORE")

          assert[String](gameCreatedLO, "ktg_learning_path_id", "lp1")
          assert[Int](gameCreatedLO, "ktg_date_dw_id", 20190625)

          val gameSkippedDf = sinks.filter(_.name == RedshiftKTGameSkippedSink).head.output
          assert(gameSkippedDf.count === 3)
          val ktGameSkippedColumns = ktGameCreatedColumns.diff(Set("ktg_id")).map(_.replace("ktg", "ktgskipped"))
          assert(gameSkippedDf.columns.toSet === ktGameSkippedColumns)

          val skippedWithLo1 = gameSkippedDf.filter("lo_uuid = 'lo-1'")
          assert[String](skippedWithLo1, "tenant_uuid", "tenantId")
          assert[String](skippedWithLo1, "student_uuid", "student-1")
          assert[String](skippedWithLo1, "subject_uuid", "subject-1")
          assert[String](skippedWithLo1, "school_uuid", "school-1")
          assert[String](skippedWithLo1, "grade_uuid", "grade-1")
          assert[String](skippedWithLo1, "section_uuid", "section-1")
          assert[Int](skippedWithLo1, "ktgskipped_num_key_terms", 4)
          assert[Long](skippedWithLo1, "ktgskipped_kt_collection_id", 629)
          assert[String](skippedWithLo1, "ktgskipped_trimester_id", "trimester-1")
          assert[Int](skippedWithLo1, "ktgskipped_trimester_order", 3)
          assert[String](skippedWithLo1, "ktgskipped_question_type", "MULTIPLE_CHOICE")
          assert[Int](skippedWithLo1, "ktgskipped_min_question", 6)
          assert[Int](skippedWithLo1, "ktgskipped_max_question", 30)
          assert[Int](skippedWithLo1, "ktgskipped_question_time_allotted", 8)
          assert[String](skippedWithLo1, "ktgskipped_type", "ROCKET")
          assert[String](skippedWithLo1, "academic_year_uuid", "763578c8-a546-4c74-b660-5df866a337c9")
          assert[String](skippedWithLo1, "ktgskipped_instructional_plan_id", "ip1")
          assert[String](skippedWithLo1, "ktgskipped_learning_path_id", "lp1")
          assert[String](skippedWithLo1, "class_uuid", "b4d1b4e4-9eb5-4eff-92a4-94139f4f2312")
          assert[String](skippedWithLo1, "ktgskipped_material_id", "ktd1b4e4-9eb5-4eff-92a4-94139f892300")
          assert[String](skippedWithLo1, "ktgskipped_material_type", "CORE")
          assert[Int](skippedWithLo1, "ktgskipped_date_dw_id", 20190625)

          val skippedWithLo2 = gameSkippedDf.filter("lo_uuid = 'lo-2'")
          assert[Int](skippedWithLo2, "ktgskipped_num_key_terms", -1)
          assert[String](skippedWithLo2, "ktgskipped_kt_collection_id", null)
          assert[String](skippedWithLo2, "academic_year_uuid", "763578c8-a546-4c74-b660-5df866a337c9")

          val skippedWithLo3 = gameSkippedDf.filter("lo_uuid = 'lo-3'")
          assert[Int](skippedWithLo3, "ktgskipped_num_key_terms", 3)
          assert[String](skippedWithLo3, "ktgskipped_kt_collection_id", null)
          assert[String](skippedWithLo3, "academic_year_uuid", "763578c8-a546-4c74-b660-5df866a337c9")

          val createdExpectedColDelta = (StagingKtGame.values.toSeq.diff(Seq("occurredOn"))
            ++ commonRedshiftCreatedColumns("ktg") :+ "eventdate")
            .map(c => if (c.contains("uuid")) "ktg_" + c.replace("uuid", "id") else c)

          val skippedExpectedColDelta = (StagingKtGameSkipped.values.toSeq.diff(Seq("occurredOn"))
            ++ commonRedshiftCreatedColumns("ktgskipped") :+ "eventdate")
            .map(c => if (c.contains("uuid")) "ktgskipped_" + c.replace("uuid", "id") else c)

          testSinkBySinkName(sinks, DeltaKTGame, createdExpectedColDelta, 2)

          testSinkBySinkName(sinks, DeltaKTGameSkippedSource, skippedExpectedColDelta, 3)

        }
      )
    }
  }

}
