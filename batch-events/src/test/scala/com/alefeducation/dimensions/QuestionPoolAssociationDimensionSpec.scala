package com.alefeducation.dimensions

import com.alefeducation.bigdata.batch.delta.DeltaIwhSink
import com.alefeducation.bigdata.commons.testutils.SparkSuite

class QuestionPoolAssociationDimensionSpec extends SparkSuite with BaseDimensionSpec {

  trait Setup {
    implicit val transformer: QuestionPoolAssociationDimension = QuestionPoolAssociationDimension(spark)
  }

  val DeltaQuestionPoolAssociationMutatedExpectedFields = Set(
    "question_pool_association_triggered_by",
    "question_pool_association_pool_id",
    "question_pool_association_question_code",
    "question_pool_association_status",
    "question_pool_association_assign_status"
  ) ++ dimDateCols("question_pool_association").toSet -- Set("question_pool_association_active_until", "question_pool_association_deleted_time")

  val redshiftColumns = List("question_pool_association_triggered_by",
    "question_pool_association_question_code",
    "question_pool_uuid",
    "question_pool_association_status",
    "question_pool_association_assign_status",
    "question_pool_association_updated_time",
    "question_pool_association_dw_updated_time",
    "question_pool_association_created_time",
    "question_pool_association_dw_created_time")


  test("QuestionsAddedToPoolEvent And QuestionsRemovedFromPoolEvent") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "parquet-question-pool-association",
          value = """
              |[
              |  {"eventType": "QuestionsAddedToPoolEvent","triggeredBy": "29","questions": [{"code": "CODE1"}, {"code": "CODE2"}],"poolId": "5f3d297a71518400010f178e","occurredOn": "2020-07-08 05:18:02.053","eventDateDw": "20200708",  "loadtime": "2020-07-08T05:18:02.053Z"},
              |  {"eventType": "QuestionsAddedToPoolEvent","triggeredBy": "29","questions": [{"code": "CODE3"}, {"code": "CODE2"}],"poolId": "5f3d297a71518400010f178e","occurredOn": "2020-07-08 05:19:02.053","eventDateDw": "20200708",  "loadtime": "2020-07-08T05:18:02.053Z"},
              |  {"eventType": "QuestionsRemovedFromPoolEvent","triggeredBy": "29","questions": [{"code": "CODE2"}],"poolId": "5f3d297a71518400010f178e","occurredOn": "2020-07-08 05:20:02.053","eventDateDw": "20200708",  "loadtime": "2020-07-08T05:18:02.053Z"},
              |  {"eventType": "QuestionsAddedToPoolEvent","triggeredBy": "29","questions": [{"code": "CODE4"}, {"code": "CODE5"}],"poolId": "5f3d297a71518400010f178e","occurredOn": "2020-07-08 05:21:02.053","eventDateDw": "20200708",  "loadtime": "2020-07-08T05:18:02.053Z"},
              |  {"eventType": "QuestionsRemovedFromPoolEvent","triggeredBy": "29","questions": [{"code": "CODE3"}],"poolId": "5f3d297a71518400010f178e","occurredOn": "2020-07-08 05:22:02.053","eventDateDw": "20200708",  "loadtime": "2020-07-08T05:18:02.053Z"},
              |  {"eventType": "QuestionsRemovedFromPoolEvent","triggeredBy": "29","questions": [{"code": "CODE3"}],"poolId": "5f3d297a71518400010f178e","occurredOn": "2020-07-08 05:23:02.053","eventDateDw": "20200708",  "loadtime": "2020-07-08T05:18:02.053Z"},
              |  {"eventType": "QuestionsAddedToPoolEvent","triggeredBy": "29","questions": [{"code": "CODE6"}, {"code": "CODE7"}],"poolId": "5f3d297a71518400010f178e","occurredOn": "2020-07-08 05:24:02.053","eventDateDw": "20200708",  "loadtime": "2020-07-08T05:18:02.053Z"},
              |  {"eventType": "QuestionsAddedToPoolEvent","triggeredBy": "29","questions": [{"code": "CODE8"}, {"code": "CODE9"}],"poolId": "5f3d297a71518400010f178e","occurredOn": "2020-07-08 05:25:02.053","eventDateDw": "20200708",  "loadtime": "2020-07-08T05:18:02.053Z"},
              |  {"eventType": "QuestionsAddedToPoolEvent","triggeredBy": "29","questions": [{"code": "CODE8"}, {"code": "CODE10"}],"poolId": "5f3d297a71518400010f178e","occurredOn": "2020-07-08 05:26:02.053","eventDateDw": "20200708",  "loadtime": "2020-07-08T05:18:02.053Z"},
              |  {"eventType": "QuestionsRemovedFromPoolEvent","triggeredBy": "29","questions": [{"code": "CODE10"}],"poolId": "5f3d297a71518400010f178e","occurredOn": "2020-07-08 05:27:02.053","eventDateDw": "20200708",  "loadtime": "2020-07-08T05:18:02.053Z"},
              |  {"eventType": "QuestionsRemovedFromPoolEvent","triggeredBy": "29","questions": [{"code": "CODE6"}],"poolId": "5f3d297a71518400010f178e","occurredOn": "2020-07-08 05:28:02.053","eventDateDw": "20200708",  "loadtime": "2020-07-08T05:18:02.053Z"}
              |]
              |""".stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val expectedPqCols = Seq(
            "poolId",
            "eventDateDw",
            "eventType",
            "loadtime",
            "triggeredBy",
            "occurredOn",
            "questions",
            "eventdate"
          )
          val deltaIWHSink = sinks.collectFirst { case s: DeltaIwhSink if s.name == "delta-question-pool-association-sink" => s }.get

          testSinkBySinkName(sinks, "parquet-question-pool-association", expectedPqCols, 11)
          testSinkBySinkName(sinks, "delta-question-pool-association-sink", DeltaQuestionPoolAssociationMutatedExpectedFields.toList, 14)
          testSinkBySinkName(sinks, "redshift-question-pool-association-sink", redshiftColumns.toList, 14)
        }
      )
    }
  }
}
