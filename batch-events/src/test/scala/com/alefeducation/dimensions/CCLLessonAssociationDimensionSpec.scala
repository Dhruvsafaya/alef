package com.alefeducation.dimensions

import com.alefeducation.bigdata.batch.delta.DeltaIwhSink
import com.alefeducation.bigdata.commons.testutils.ExpectedFields.{ExpectedField, assertExpectedFields}
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Constants._
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, IntegerType, LongType, StringType}
import org.scalatest.matchers.must.Matchers


class CCLLessonAssociationDimensionSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer: CCLLessonAssociationDimension = new CCLLessonAssociationDimension(spark)
  }

  val ExpectedTimestampFields: List[ExpectedField] = getCommonExpectedWoDelTimestampFields(LearningObjectiveAssociationEntity)

  val expectedColumns: List[ExpectedField] = List(
    ExpectedField(name = s"${LearningObjectiveAssociationEntity}_lo_id", dataType = StringType),
    ExpectedField(name = s"${LearningObjectiveAssociationEntity}_id", dataType = StringType),
    ExpectedField(name = s"${LearningObjectiveAssociationEntity}_lo_ccl_id", dataType = LongType),
    ExpectedField(name = s"${LearningObjectiveAssociationEntity}_status", dataType = IntegerType),
    ExpectedField(name = s"${LearningObjectiveAssociationEntity}_attach_status", dataType = IntegerType),
    ExpectedField(name = s"${LearningObjectiveAssociationEntity}_type", dataType = IntegerType)
  ) ++ ExpectedTimestampFields

  val expectedSkillColumns: List[ExpectedField] =
    ExpectedField(name = s"${LearningObjectiveAssociationEntity}_derived", dataType = BooleanType) :: expectedColumns

  val expectedOutcomeColumns: List[ExpectedField] = expectedColumns

  test("in Lesson skill link") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonSkillLinkSource,
          value = """
                    |{
                    |  "eventType": "LessonSkillLinkedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "skillId": "b42765ca-3f4c-44fb-8ec7-f96042c93703",
                    |  "derived": false,
                    |  "occurredOn":"2020-07-20 02:40:00.0"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquetDf = sinks
            .find(_.name == ParquetCCLLessonSkillLinkSink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonSkillLinkSink is not found"))

          assert[Long](parquetDf, "lessonId", 4128)
          assert[String](parquetDf, "lessonUuid", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[String](parquetDf, "skillId", "b42765ca-3f4c-44fb-8ec7-f96042c93703")
          assert[Boolean](parquetDf, "derived", false)


          val redshiftDf = sinks
            .find(_.name == RedshiftLearningObjectiveAssociationDimSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$RedshiftLearningObjectiveAssociationDimSink is not found"))

          assertExpectedFields(redshiftDf.schema.fields.toList, expectedSkillColumns)
          assert[String](redshiftDf, s"${LearningObjectiveAssociationEntity}_lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](redshiftDf, s"${LearningObjectiveAssociationEntity}_lo_ccl_id", 4128)
          assert[String](redshiftDf, s"${LearningObjectiveAssociationEntity}_id", "b42765ca-3f4c-44fb-8ec7-f96042c93703")
          assert[Boolean](redshiftDf, s"${LearningObjectiveAssociationEntity}_derived", false)
          assert[Long](redshiftDf, s"${LearningObjectiveAssociationEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveAssociationEntity}_attach_status", LessonAssociationAttachedStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveAssociationEntity}_type", LessonSkillLinkType)


          assert[String](redshiftDf, s"${LearningObjectiveAssociationEntity}_created_time", "2020-07-20 02:40:00.0")
          assert[String](redshiftDf, s"${LearningObjectiveAssociationEntity}_updated_time", null)

          val deltaIwhSink = sinks.collect {
            case x: DeltaIwhSink => x
          }.head

          val deltaIwhDf = deltaIwhSink.df

          assertExpectedFields(deltaIwhDf.schema.fields.toList, expectedSkillColumns)

          deltaIwhSink.matchConditions must include(transformer.deltaMatchConditions)

          assert[String](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_lo_ccl_id", 4128)
          assert[String](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_id", "b42765ca-3f4c-44fb-8ec7-f96042c93703")
          assert[Boolean](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_derived", false)
          assert[Long](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_attach_status", LessonAssociationAttachedStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_type", LessonSkillLinkType)
        }
      )
    }
  }

  test("in Lesson skill unlink") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonSkillLinkSource,
          value = """
                    |{
                    |  "eventType": "LessonSkillUnlinkedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "skillId":"b42765ca-3f4c-44fb-8ec7-f96042c93703",
                    |  "derived":false,
                    |  "occurredOn":"2020-07-20 02:40:00.0"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquetDf = sinks
            .find(_.name == ParquetCCLLessonSkillLinkSink)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonSkillLinkSink is not found"))

          assert[Long](parquetDf, "lessonId", 4128)
          assert[String](parquetDf, "lessonUuid", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[String](parquetDf, "skillId", "b42765ca-3f4c-44fb-8ec7-f96042c93703")
          assert[Boolean](parquetDf, "derived", false)


          val redshiftDf = sinks
            .find(_.name == RedshiftLearningObjectiveAssociationDimSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$RedshiftLearningObjectiveAssociationDimSink is not found"))

          assertExpectedFields(redshiftDf.schema.fields.toList, expectedSkillColumns)
          assert[String](redshiftDf, s"${LearningObjectiveAssociationEntity}_lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](redshiftDf, s"${LearningObjectiveAssociationEntity}_lo_ccl_id", 4128)
          assert[String](redshiftDf, s"${LearningObjectiveAssociationEntity}_id", "b42765ca-3f4c-44fb-8ec7-f96042c93703")
          assert[Boolean](redshiftDf, s"${LearningObjectiveAssociationEntity}_derived", false)
          assert[Long](redshiftDf, s"${LearningObjectiveAssociationEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveAssociationEntity}_attach_status", LessonAssociationDetachedStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveAssociationEntity}_type", LessonSkillLinkType)


          assert[String](redshiftDf, s"${LearningObjectiveAssociationEntity}_created_time", "2020-07-20 02:40:00.0")
          assert[String](redshiftDf, s"${LearningObjectiveAssociationEntity}_updated_time", null)

          val deltaIwhSink = sinks.collect {
            case x: DeltaIwhSink => x
          }.head

          val deltaIwhDf = deltaIwhSink.df

          assertExpectedFields(deltaIwhDf.schema.fields.toList, expectedSkillColumns)

          deltaIwhSink.matchConditions must include(transformer.deltaMatchConditions)

          assert[String](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_lo_ccl_id", 4128)
          assert[String](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_id", "b42765ca-3f4c-44fb-8ec7-f96042c93703")
          assert[Boolean](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_derived", false)
          assert[Long](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_attach_status", LessonAssociationDetachedStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_type", LessonSkillLinkType)
        }
      )
    }
  }

  test("in Lesson outcome attached") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonOutcomeAttachSource,
          value = """
                    |{
                    |  "eventType": "LessonOutcomeAttachedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "outcomeId":"substandard-793672",
                    |  "occurredOn":"2020-07-20 02:40:00.0"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquetDf = sinks
            .find(_.name == ParquetCCLLessonOutcomeAttachSource)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonOutcomeAttachSource is not found"))

          assert[Long](parquetDf, "lessonId", 4128)
          assert[String](parquetDf, "lessonUuid", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[String](parquetDf, "outcomeId", "substandard-793672")


          val redshiftDf = sinks
            .find(_.name == RedshiftLearningObjectiveAssociationDimSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$RedshiftLearningObjectiveAssociationDimSink is not found"))

          assertExpectedFields(redshiftDf.schema.fields.toList, expectedOutcomeColumns)
          assert[String](redshiftDf, s"${LearningObjectiveAssociationEntity}_lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](redshiftDf, s"${LearningObjectiveAssociationEntity}_lo_ccl_id", 4128)
          assert[String](redshiftDf, s"${LearningObjectiveAssociationEntity}_id", "substandard-793672")
          assert[Long](redshiftDf, s"${LearningObjectiveAssociationEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveAssociationEntity}_attach_status", LessonAssociationAttachedStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveAssociationEntity}_type", LessonOutcomeAssociationType)


          assert[String](redshiftDf, s"${LearningObjectiveAssociationEntity}_created_time", "2020-07-20 02:40:00.0")
          assert[String](redshiftDf, s"${LearningObjectiveAssociationEntity}_updated_time", null)

          val deltaIwhSink = sinks.collect {
            case x: DeltaIwhSink => x
          }.head

          val deltaIwhDf = deltaIwhSink.df

          assertExpectedFields(deltaIwhDf.schema.fields.toList, expectedOutcomeColumns)

          deltaIwhSink.matchConditions must include(transformer.deltaMatchConditions)

          assert[String](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_lo_ccl_id", 4128)
          assert[String](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_id", "substandard-793672")
          assert[Long](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_attach_status", LessonAssociationAttachedStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_type", LessonOutcomeAssociationType)
        }
      )
    }
  }

  test("in Lesson outcome detached") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonOutcomeAttachSource,
          value = """
                    |{
                    |  "eventType": "LessonOutcomeDetachedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "outcomeId":"substandard-793672",
                    |  "occurredOn":"2020-07-20 02:40:00.0"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquetDf = sinks
            .find(_.name == ParquetCCLLessonOutcomeAttachSource)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonOutcomeAttachSource is not found"))

          assert[Long](parquetDf, "lessonId", 4128)
          assert[String](parquetDf, "lessonUuid", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[String](parquetDf, "outcomeId", "substandard-793672")


          val redshiftDf = sinks
            .find(_.name == RedshiftLearningObjectiveAssociationDimSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$RedshiftLearningObjectiveAssociationDimSink is not found"))

          assertExpectedFields(redshiftDf.schema.fields.toList, expectedOutcomeColumns)
          assert[String](redshiftDf, s"${LearningObjectiveAssociationEntity}_lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](redshiftDf, s"${LearningObjectiveAssociationEntity}_lo_ccl_id", 4128)
          assert[String](redshiftDf, s"${LearningObjectiveAssociationEntity}_id", "substandard-793672")
          assert[Long](redshiftDf, s"${LearningObjectiveAssociationEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveAssociationEntity}_attach_status", LessonAssociationDetachedStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveAssociationEntity}_type", LessonOutcomeAssociationType)


          assert[String](redshiftDf, s"${LearningObjectiveAssociationEntity}_created_time", "2020-07-20 02:40:00.0")
          assert[String](redshiftDf, s"${LearningObjectiveAssociationEntity}_updated_time", null)

          val deltaIwhSink = sinks.collect {
            case x: DeltaIwhSink => x
          }.head

          val deltaIwhDf = deltaIwhSink.df

          assertExpectedFields(deltaIwhDf.schema.fields.toList, expectedOutcomeColumns)

          deltaIwhSink.matchConditions must include(transformer.deltaMatchConditions)

          assert[String](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_lo_ccl_id", 4128)
          assert[String](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_id", "substandard-793672")
          assert[Long](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_attach_status", LessonAssociationDetachedStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveAssociationEntity}_type", LessonOutcomeAssociationType)
        }
      )
    }
  }

  test("in Lesson skill linked and unlinked") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonSkillLinkSource,
          value = """
                    |[
                    |{
                    |  "eventType": "LessonSkillLinkedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "skillId":"b42765ca-3f4c-44fb-8ec7-f96042c93703",
                    |  "derived":false,
                    |  "occurredOn":"2020-07-20 02:40:00.0"
                    |},
                    |{
                    |  "eventType": "LessonSkillUnlinkedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "skillId":"b42765ca-3f4c-44fb-8ec7-f96042c93703",
                    |  "derived":false,
                    |  "occurredOn":"2020-07-20 02:41:00.0"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val deltaIwhDf = sinks.collect {
            case x: DeltaIwhSink => x
          }.map(_.output).head

          val statuses = deltaIwhDf.orderBy(col("lo_association_created_time"))
            .select(col("lo_association_status"), col("lo_association_attach_status"))
            .collect()
            .map(x => (x.getInt(0), x.getInt(1)))
            .toList

          assert(statuses == List(
            (LessonAssociationInactiveStatusVal, LessonAssociationAttachedStatusVal),
            (LessonAssociationActiveStatusVal, LessonAssociationDetachedStatusVal))
          )
        }
      )
    }
  }

}
