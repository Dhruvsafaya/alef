package com.alefeducation.dimensions

import com.alefeducation.bigdata.batch.delta.{Alias, DeltaIwhSink}
import com.alefeducation.bigdata.commons.testutils.ExpectedFields.{ExpectedField, assertExpectedFields}
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Constants._
import com.alefeducation.util.Helpers.{
  LearningObjectiveStepInstanceEntity,
  ParquetCCLLessonAssigmentAttachSource,
  ParquetCCLLessonAssessmentRuleSource,
  ParquetCCLLessonContentAttachSource,
  RedshiftLessonStepInstanceSink
}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.scalatest.matchers.must.Matchers

class CCLLessonStepInstanceDimensionSpec extends SparkSuite with Matchers {
  trait Setup {
    implicit val transformer: CCLLessonStepInstanceDimension = new CCLLessonStepInstanceDimension(spark)
  }

  val ExpectedTimestampFields: List[ExpectedField] = getCommonExpectedWoDelTimestampFields(LearningObjectiveStepInstanceEntity)

  val expectedColumns: List[ExpectedField] = List(
    ExpectedField(name = s"${LearningObjectiveStepInstanceEntity}_lo_id", dataType = StringType),
    ExpectedField(name = s"${LearningObjectiveStepInstanceEntity}_id", dataType = StringType),
    ExpectedField(name = s"${LearningObjectiveStepInstanceEntity}_step_id", dataType = LongType),
    ExpectedField(name = s"${LearningObjectiveStepInstanceEntity}_step_uuid", dataType = StringType),
    ExpectedField(name = s"${LearningObjectiveStepInstanceEntity}_lo_ccl_id", dataType = LongType),
    ExpectedField(name = s"${LearningObjectiveStepInstanceEntity}_status", dataType = IntegerType),
    ExpectedField(name = s"${LearningObjectiveStepInstanceEntity}_attach_status", dataType = IntegerType),
    ExpectedField(name = s"${LearningObjectiveStepInstanceEntity}_type", dataType = IntegerType)
  ) ++ ExpectedTimestampFields

  val expectedAssessmentColumns: List[ExpectedField] = expectedColumns ++ List(
    ExpectedField(name = s"${LearningObjectiveStepInstanceEntity}_pool_id", dataType = StringType),
    ExpectedField(name = s"${LearningObjectiveStepInstanceEntity}_pool_name", dataType = StringType),
    ExpectedField(name = s"${LearningObjectiveStepInstanceEntity}_difficulty_level", dataType = StringType),
    ExpectedField(name = s"${LearningObjectiveStepInstanceEntity}_resource_type", dataType = StringType),
    ExpectedField(name = s"${LearningObjectiveStepInstanceEntity}_questions", dataType = LongType)
  )

  val updatedFields: Map[String, String] = Map(
    s"${LearningObjectiveStepInstanceEntity}_status" -> s"4",
    s"${LearningObjectiveStepInstanceEntity}_updated_time" -> s"${Alias.Events}.${LearningObjectiveStepInstanceEntity}_created_time",
    s"${LearningObjectiveStepInstanceEntity}_dw_updated_time" -> s"current_timestamp"
  )

  test("Lesson content attached") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonContentAttachSource,
          value = """
                    |{
                    |  "eventType": "LessonContentAttachedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "contentId":33260,
                    |  "stepId":1,
                    |  "stepUuid": "1ca1ab16-232c-42fc-9537-b9fe35fd207b",
                    |  "occurredOn":"2020-07-20 02:40:00.0"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetDf = sinks
            .find(_.name == ParquetCCLLessonContentAttachSource)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonContentAttachSource is not found"))

          assert[Long](parquetDf, "lessonId", 4128)
          assert[String](parquetDf, "lessonUuid", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[String](parquetDf, "stepUuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](parquetDf, "contentId", 33260)
          assert[Long](parquetDf, "stepId", 1)

          val redshiftDf = sinks
            .find(_.name == RedshiftLessonStepInstanceSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$RedshiftLessonStepInstanceSink is not found"))

          assertExpectedFields(redshiftDf.schema.fields.toList, expectedColumns)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_lo_ccl_id", 4128)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_id", "33260")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_uuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_attach_status", LessonAssociationAttachedStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_type", LessonContentAssociationType)

          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_created_time", "2020-07-20 02:40:00.0")
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_updated_time", null)

          val deltaIwhSink = sinks.collect {
            case x: DeltaIwhSink => x
          }.head

          val deltaIwhDf = deltaIwhSink.df

          assertExpectedFields(deltaIwhDf.schema.fields.toList, expectedColumns)

          deltaIwhSink.matchConditions must include(transformer.stepInstanceDeltaMatchConditions)
          assert(deltaIwhSink.updateFields === updatedFields)

          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_lo_ccl_id", 4128)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_id", "33260")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_uuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_attach_status", LessonAssociationAttachedStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_type", LessonContentAssociationType)
        }
      )
    }
  }

  test("Lesson content attached without stepUuid") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonContentAttachSource,
          value = """
                    |{
                    |  "eventType": "LessonContentAttachedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "contentId":33260,
                    |  "stepId":1,
                    |  "occurredOn":"2020-07-20 02:40:00.0"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val redshiftDf = sinks
            .find(_.name == RedshiftLessonStepInstanceSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$RedshiftLessonStepInstanceSink is not found"))

          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_uuid", null)

          val deltaIwhSink = sinks.collect {
            case x: DeltaIwhSink => x
          }.head

          val deltaIwhDf = deltaIwhSink.df

          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_uuid", null)
        }
      )
    }
  }

  test("Lesson content detached") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonContentAttachSource,
          value = """
                    |{
                    |  "eventType": "LessonContentDetachedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "contentId":33260,
                    |  "stepId":1,
                    |  "stepUuid": "1ca1ab16-232c-42fc-9537-b9fe35fd207b",
                    |  "occurredOn":"2020-07-20 02:40:00.0"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetDf = sinks
            .find(_.name == ParquetCCLLessonContentAttachSource)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonContentAttachSource is not found"))

          assert[Long](parquetDf, "lessonId", 4128)
          assert[String](parquetDf, "lessonUuid", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](parquetDf, "contentId", 33260)
          assert[Long](parquetDf, "stepId", 1)

          val redshiftDf = sinks
            .find(_.name == RedshiftLessonStepInstanceSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$RedshiftLessonStepInstanceSink is not found"))

          assertExpectedFields(redshiftDf.schema.fields.toList, expectedColumns)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_lo_ccl_id", 4128)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_id", "33260")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_uuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_attach_status", LessonAssociationDetachedStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_type", LessonContentAssociationType)

          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_created_time", "2020-07-20 02:40:00.0")
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_updated_time", null)

          val deltaIwhSink = sinks.collect {
            case x: DeltaIwhSink => x
          }.head

          val deltaIwhDf = deltaIwhSink.df

          assertExpectedFields(deltaIwhDf.schema.fields.toList, expectedColumns)

          deltaIwhSink.matchConditions must include(transformer.stepInstanceDeltaMatchConditions)
          assert(deltaIwhSink.updateFields === updatedFields)

          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_lo_ccl_id", 4128)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_id", "33260")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_uuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_attach_status", LessonAssociationDetachedStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_type", LessonContentAssociationType)
        }
      )
    }
  }

  test("Lesson content attached and detached") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonContentAttachSource,
          value = """
                    |[
                    |{
                    |  "eventType": "LessonContentAttachedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "contentId":33260,
                    |  "stepId":1,
                    |  "stepUuid": "1ca1ab16-232c-42fc-9537-b9fe35fd207b",
                    |  "occurredOn":"2020-07-20 02:40:00.0"
                    |},
                    |{
                    |  "eventType": "LessonContentDetachedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "contentId":33260,
                    |  "stepId":1,
                    |  "occurredOn":"2020-07-20 02:41:00.0"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val deltaIwhDf = sinks
            .collect {
              case x: DeltaIwhSink => x
            }
            .map(_.output)
            .head

          val statuses = deltaIwhDf
            .orderBy(col(s"${LearningObjectiveStepInstanceEntity}_created_time"))
            .select(col(s"${LearningObjectiveStepInstanceEntity}_status"), col(s"${LearningObjectiveStepInstanceEntity}_attach_status"))
            .collect()
            .map(x => (x.getInt(0), x.getInt(1)))
            .toList

          assert(
            statuses == List((LessonAssociationInactiveStatusVal, LessonAssociationAttachedStatusVal),
                             (LessonAssociationActiveStatusVal, LessonAssociationDetachedStatusVal)))
        }
      )
    }
  }

  test("Lesson assignment attached") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonAssigmentAttachSource,
          value = """
                    |{
                    |  "eventType": "LessonAssignmentAttachedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "assignmentId": "995afcba-e214-4d3c-a84a-000000234567",
                    |  "stepId":1,
                    |  "stepUuid": "1ca1ab16-232c-42fc-9537-b9fe35fd207b",
                    |  "occurredOn":"2020-07-20 02:40:00.0"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetDf = sinks
            .find(_.name == ParquetCCLLessonAssigmentAttachSource)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonAssigmentAttachSource is not found"))

          assert[Long](parquetDf, "lessonId", 4128)
          assert[String](parquetDf, "lessonUuid", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[String](parquetDf, "assignmentId", "995afcba-e214-4d3c-a84a-000000234567")
          assert[String](parquetDf, "stepUuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](parquetDf, "stepId", 1)

          val redshiftDf = sinks
            .find(_.name == RedshiftLessonStepInstanceSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$RedshiftLessonStepInstanceSink is not found"))

          assertExpectedFields(redshiftDf.schema.fields.toList, expectedColumns)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_lo_ccl_id", 4128)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_id", "995afcba-e214-4d3c-a84a-000000234567")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_uuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_attach_status", LessonAssociationAttachedStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_type", LessonAssigmentAssociationType)

          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_created_time", "2020-07-20 02:40:00.0")
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_updated_time", null)

          val deltaIwhSink = sinks.collect {
            case x: DeltaIwhSink => x
          }.head

          val deltaIwhDf = deltaIwhSink.df

          assertExpectedFields(deltaIwhDf.schema.fields.toList, expectedColumns)

          deltaIwhSink.matchConditions must include(transformer.stepInstanceDeltaMatchConditions)
          assert(deltaIwhSink.updateFields === updatedFields)

          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_lo_ccl_id", 4128)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_id", "995afcba-e214-4d3c-a84a-000000234567")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_uuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_attach_status", LessonAssociationAttachedStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_type", LessonAssigmentAssociationType)
        }
      )
    }
  }

  test("Lesson assignment detached") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonAssigmentAttachSource,
          value = """
                    |{
                    |  "eventType": "LessonAssignmentDetachedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "assignmentId":"995afcba-e214-4d3c-a84a-000000234567",
                    |  "stepId":1,
                    |  "stepUuid": "1ca1ab16-232c-42fc-9537-b9fe35fd207b",
                    |  "occurredOn":"2020-07-20 02:40:00.0"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetDf = sinks
            .find(_.name == ParquetCCLLessonAssigmentAttachSource)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonAssigmentAttachSource is not found"))

          assert[Long](parquetDf, "lessonId", 4128)
          assert[String](parquetDf, "lessonUuid", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[String](parquetDf, "assignmentId", "995afcba-e214-4d3c-a84a-000000234567")
          assert[String](parquetDf, "stepUuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](parquetDf, "stepId", 1)

          val redshiftDf = sinks
            .find(_.name == RedshiftLessonStepInstanceSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$RedshiftLessonStepInstanceSink is not found"))

          assertExpectedFields(redshiftDf.schema.fields.toList, expectedColumns)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_lo_ccl_id", 4128)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_id", "995afcba-e214-4d3c-a84a-000000234567")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_uuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_attach_status", LessonAssociationDetachedStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_type", LessonAssigmentAssociationType)

          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_created_time", "2020-07-20 02:40:00.0")
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_updated_time", null)

          val deltaIwhSink = sinks.collect {
            case x: DeltaIwhSink => x
          }.head

          val deltaIwhDf = deltaIwhSink.df

          assertExpectedFields(deltaIwhDf.schema.fields.toList, expectedColumns)

          deltaIwhSink.matchConditions must include(transformer.stepInstanceDeltaMatchConditions)
          assert(deltaIwhSink.updateFields === updatedFields)

          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_lo_id", "f22b5423-c60e-4c0d-b06d-000000004128")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_lo_ccl_id", 4128)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_id", "995afcba-e214-4d3c-a84a-000000234567")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_uuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_attach_status", LessonAssociationDetachedStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_type", LessonAssigmentAssociationType)
        }
      )
    }
  }

  test("Lesson assignment attached and detached") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonAssigmentAttachSource,
          value = """
                    |[
                    |{
                    |  "eventType": "LessonAssignmentAttachedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "assignmentId":"995afcba-e214-4d3c-a84a-000000234567",
                    |  "stepId":1,
                    |  "stepUuid": "1ca1ab16-232c-42fc-9537-b9fe35fd207b",
                    |  "occurredOn":"2020-07-20 02:40:00.0"
                    |},
                    |{
                    |  "eventType": "LessonAssignmentDetachedEvent",
                    |  "lessonId":4128,
                    |  "lessonUuid":"f22b5423-c60e-4c0d-b06d-000000004128",
                    |  "assignmentId":"995afcba-e214-4d3c-a84a-000000234567",
                    |  "stepId":1,
                    |  "stepUuid": "1ca1ab16-232c-42fc-9537-b9fe35fd207b",
                    |  "occurredOn":"2020-07-20 02:41:00.0"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val deltaIwhDf = sinks
            .collect {
              case x: DeltaIwhSink => x
            }
            .map(_.output)
            .head

          val statuses = deltaIwhDf
            .orderBy(col(s"${LearningObjectiveStepInstanceEntity}_created_time"))
            .select(col(s"${LearningObjectiveStepInstanceEntity}_status"), col(s"${LearningObjectiveStepInstanceEntity}_attach_status"))
            .collect()
            .map(x => (x.getInt(0), x.getInt(1)))
            .toList

          assert(
            statuses == List((LessonAssociationInactiveStatusVal, LessonAssociationAttachedStatusVal),
                             (LessonAssociationActiveStatusVal, LessonAssociationDetachedStatusVal)))
        }
      )
    }
  }

  test("Lesson assessment added") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonAssessmentRuleSource,
          value = """
                    |{
                    |  "eventType": "LessonAssessmentRuleAddedEvent",
                    |  "lessonId": 234567,
                    |  "lessonUuid": "995afcba-e214-4d3c-a84a-000000234567",
                    |  "stepUuid": "1ca1ab16-232c-42fc-9537-b9fe35fd207b",
                    |  "stepId": 1,
                    |  "rule": {
                    |     "id": "e07d4879-23be-4854-bd59-0a7647bd3f6b",
                    |     "poolId": "5f43c5f70e9eb20001907460",
                    |     "poolName": "pool 1",
                    |     "difficultyLevel": "EASY",
                    |     "resourceType": "TEQ1",
                    |     "questions": 4
                    |  },
                    |  "occurredOn":"2020-07-20 02:40:00.0"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetDf = sinks
            .find(_.name == ParquetCCLLessonAssessmentRuleSource)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonAssessmentRuleSource is not found"))

          assert[Long](parquetDf, "lessonId", 234567)
          assert[String](parquetDf, "lessonUuid", "995afcba-e214-4d3c-a84a-000000234567")
          assert[Long](parquetDf, "stepId", 1)
          assert[String](parquetDf, "stepUuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          val parquetRuleDf = parquetDf.select("rule.*")
          assert[String](parquetRuleDf, "id", "e07d4879-23be-4854-bd59-0a7647bd3f6b")
          assert[String](parquetRuleDf, "poolId", "5f43c5f70e9eb20001907460")
          assert[String](parquetRuleDf, "poolName", "pool 1")
          assert[String](parquetRuleDf, "difficultyLevel", "EASY")
          assert[String](parquetRuleDf, "resourceType", "TEQ1")
          assert[Long](parquetRuleDf, "questions", 4)

          val redshiftDf = sinks
            .find(_.name == RedshiftLessonStepInstanceSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$RedshiftLessonStepInstanceSink is not found"))

          assertExpectedFields(redshiftDf.schema.fields.toList, expectedAssessmentColumns)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_lo_id", "995afcba-e214-4d3c-a84a-000000234567")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_lo_ccl_id", 234567)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_id", "e07d4879-23be-4854-bd59-0a7647bd3f6b")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_uuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_attach_status", LessonAssociationAttachedStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_type", LessonAssessmentRuleAssociationType)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_pool_id", "5f43c5f70e9eb20001907460")
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_pool_name", "pool 1")
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_difficulty_level", "EASY")
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_resource_type", "TEQ1")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_questions", 4)

          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_created_time", "2020-07-20 02:40:00.0")
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_updated_time", null)

          val deltaIwhSink = sinks.collect {
            case x: DeltaIwhSink => x
          }.head

          val deltaIwhDf = deltaIwhSink.df

          assertExpectedFields(deltaIwhDf.schema.fields.toList, expectedAssessmentColumns)

          deltaIwhSink.matchConditions must include(transformer.stepInstanceDeltaMatchConditions)
          assert(deltaIwhSink.updateFields === updatedFields)

          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_lo_id", "995afcba-e214-4d3c-a84a-000000234567")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_lo_ccl_id", 234567)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_id", "e07d4879-23be-4854-bd59-0a7647bd3f6b")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_uuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_attach_status", LessonAssociationAttachedStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_type", LessonAssessmentRuleAssociationType)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_pool_id", "5f43c5f70e9eb20001907460")
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_pool_name", "pool 1")
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_difficulty_level", "EASY")
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_resource_type", "TEQ1")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_questions", 4)
        }
      )
    }
  }

  test("Lesson assessment updated") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonAssessmentRuleSource,
          value = """
                    |{
                    |  "eventType": "LessonAssessmentRuleUpdatedEvent",
                    |  "lessonId": 234567,
                    |  "lessonUuid": "995afcba-e214-4d3c-a84a-000000234567",
                    |  "stepUuid": "1ca1ab16-232c-42fc-9537-b9fe35fd207b",
                    |  "stepId": 1,
                    |  "rule": {
                    |     "id": "e07d4879-23be-4854-bd59-0a7647bd3f6b",
                    |     "poolId": "5f43c5f70e9eb20001907460",
                    |     "poolName": "pool 22",
                    |     "difficultyLevel": "HARD",
                    |     "resourceType": "TEQ2",
                    |     "questions": 5
                    |  },
                    |  "occurredOn":"2020-07-20 02:40:00.0"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetDf = sinks
            .find(_.name == ParquetCCLLessonAssessmentRuleSource)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonAssessmentRuleSource is not found"))

          assert[Long](parquetDf, "lessonId", 234567)
          assert[String](parquetDf, "lessonUuid", "995afcba-e214-4d3c-a84a-000000234567")
          assert[Long](parquetDf, "stepId", 1)
          assert[String](parquetDf, "stepUuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          val parquetRuleDf = parquetDf.select("rule.*")
          assert[String](parquetRuleDf, "id", "e07d4879-23be-4854-bd59-0a7647bd3f6b")
          assert[String](parquetRuleDf, "poolId", "5f43c5f70e9eb20001907460")
          assert[String](parquetRuleDf, "poolName", "pool 22")
          assert[String](parquetRuleDf, "difficultyLevel", "HARD")
          assert[String](parquetRuleDf, "resourceType", "TEQ2")
          assert[Long](parquetRuleDf, "questions", 5)

          val redshiftDf = sinks
            .find(_.name == RedshiftLessonStepInstanceSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$RedshiftLessonStepInstanceSink is not found"))

          assertExpectedFields(redshiftDf.schema.fields.toList, expectedAssessmentColumns)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_lo_id", "995afcba-e214-4d3c-a84a-000000234567")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_lo_ccl_id", 234567)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_id", "e07d4879-23be-4854-bd59-0a7647bd3f6b")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_uuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_attach_status", LessonAssociationAttachedStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_type", LessonAssessmentRuleAssociationType)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_pool_id", "5f43c5f70e9eb20001907460")
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_pool_name", "pool 22")
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_difficulty_level", "HARD")
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_resource_type", "TEQ2")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_questions", 5)

          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_created_time", "2020-07-20 02:40:00.0")
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_updated_time", null)

          val deltaIwhSink = sinks.collect {
            case x: DeltaIwhSink => x
          }.head

          val deltaIwhDf = deltaIwhSink.df

          assertExpectedFields(deltaIwhDf.schema.fields.toList, expectedAssessmentColumns)

          deltaIwhSink.matchConditions must include(transformer.stepInstanceDeltaMatchConditions)
          assert(deltaIwhSink.updateFields === updatedFields)

          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_lo_id", "995afcba-e214-4d3c-a84a-000000234567")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_lo_ccl_id", 234567)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_id", "e07d4879-23be-4854-bd59-0a7647bd3f6b")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_uuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_attach_status", LessonAssociationAttachedStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_type", LessonAssessmentRuleAssociationType)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_pool_id", "5f43c5f70e9eb20001907460")
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_pool_name", "pool 22")
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_difficulty_level", "HARD")
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_resource_type", "TEQ2")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_questions", 5)
        }
      )
    }
  }

  test("Lesson assessment removed") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonAssessmentRuleSource,
          value = """
                    |{
                    |  "eventType": "LessonAssessmentRuleRemovedEvent",
                    |  "lessonId": 234567,
                    |  "lessonUuid": "995afcba-e214-4d3c-a84a-000000234567",
                    |  "stepUuid": "1ca1ab16-232c-42fc-9537-b9fe35fd207b",
                    |  "stepId": 1,
                    |  "rule": {
                    |     "id": "e07d4879-23be-4854-bd59-0a7647bd3f6b",
                    |     "poolId": "5f43c5f70e9eb20001907460",
                    |     "poolName": "pool 1",
                    |     "difficultyLevel": "EASY",
                    |     "resourceType": "TEQ1",
                    |     "questions": 4
                    |  },
                    |  "occurredOn":"2020-07-20 02:40:00.0"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetDf = sinks
            .find(_.name == ParquetCCLLessonAssessmentRuleSource)
            .map(_.output)
            .getOrElse(throw new RuntimeException(s"$ParquetCCLLessonAssessmentRuleSource is not found"))

          assert[Long](parquetDf, "lessonId", 234567)
          assert[String](parquetDf, "lessonUuid", "995afcba-e214-4d3c-a84a-000000234567")
          assert[Long](parquetDf, "stepId", 1)
          assert[String](parquetDf, "stepUuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          val parquetRuleDf = parquetDf.select("rule.*")
          assert[String](parquetRuleDf, "id", "e07d4879-23be-4854-bd59-0a7647bd3f6b")
          assert[String](parquetRuleDf, "poolId", "5f43c5f70e9eb20001907460")
          assert[String](parquetRuleDf, "poolName", "pool 1")
          assert[String](parquetRuleDf, "difficultyLevel", "EASY")
          assert[String](parquetRuleDf, "resourceType", "TEQ1")
          assert[Long](parquetRuleDf, "questions", 4)

          val redshiftDf = sinks
            .find(_.name == RedshiftLessonStepInstanceSink)
            .map(_.output)
            .getOrElse(throw new AssertionError(s"$RedshiftLessonStepInstanceSink is not found"))

          assertExpectedFields(redshiftDf.schema.fields.toList, expectedAssessmentColumns)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_lo_id", "995afcba-e214-4d3c-a84a-000000234567")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_lo_ccl_id", 234567)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_id", "e07d4879-23be-4854-bd59-0a7647bd3f6b")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_step_uuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_attach_status", LessonAssociationDetachedStatusVal)
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_type", LessonAssessmentRuleAssociationType)
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_pool_id", "5f43c5f70e9eb20001907460")
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_pool_name", "pool 1")
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_difficulty_level", "EASY")
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_resource_type", "TEQ1")
          assert[Long](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_questions", 4)

          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_created_time", "2020-07-20 02:40:00.0")
          assert[String](redshiftDf, s"${LearningObjectiveStepInstanceEntity}_updated_time", null)

          val deltaIwhSink = sinks.collect {
            case x: DeltaIwhSink => x
          }.head

          val deltaIwhDf = deltaIwhSink.df

          assertExpectedFields(deltaIwhDf.schema.fields.toList, expectedAssessmentColumns)

          deltaIwhSink.matchConditions must include(transformer.stepInstanceDeltaMatchConditions)
          assert(deltaIwhSink.updateFields === updatedFields)

          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_lo_id", "995afcba-e214-4d3c-a84a-000000234567")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_lo_ccl_id", 234567)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_id", "e07d4879-23be-4854-bd59-0a7647bd3f6b")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_id", 1)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_step_uuid", "1ca1ab16-232c-42fc-9537-b9fe35fd207b")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_status", LessonAssociationActiveStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_attach_status", LessonAssociationDetachedStatusVal)
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_type", LessonAssessmentRuleAssociationType)
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_pool_id", "5f43c5f70e9eb20001907460")
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_pool_name", "pool 1")
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_difficulty_level", "EASY")
          assert[String](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_resource_type", "TEQ1")
          assert[Long](deltaIwhDf, s"${LearningObjectiveStepInstanceEntity}_questions", 4)
        }
      )
    }
  }

  test("Lesson assessment added, updated and removed") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLLessonAssessmentRuleSource,
          value = """
                    |[
                    |{
                    |  "eventType": "LessonAssessmentRuleAddedEvent",
                    |  "lessonId": 234567,
                    |  "lessonUuid": "995afcba-e214-4d3c-a84a-000000234567",
                    |  "stepUuid": "1ca1ab16-232c-42fc-9537-b9fe35fd207b",
                    |  "stepId": 1,
                    |  "rule": {
                    |     "id": "e07d4879-23be-4854-bd59-0a7647bd3f6b",
                    |     "poolId": "5f43c5f70e9eb20001907460",
                    |     "poolName": "pool 1",
                    |     "difficultyLevel": "EASY",
                    |     "resourceType": "TEQ1",
                    |     "questions": 4
                    |  },
                    |  "occurredOn":"2020-07-20 02:40:00.0"
                    |},
                    |{
                    |  "eventType": "LessonAssessmentRuleUpdatedEvent",
                    |  "lessonId": 234567,
                    |  "lessonUuid": "995afcba-e214-4d3c-a84a-000000234567",
                    |  "stepUuid": "1ca1ab16-232c-42fc-9537-b9fe35fd207b",
                    |  "stepId": 1,
                    |  "rule": {
                    |     "id": "e07d4879-23be-4854-bd59-0a7647bd3f6b",
                    |     "poolId": "5f43c5f70e9eb20001907460",
                    |     "poolName": "pool 22",
                    |     "difficultyLevel": "HARD",
                    |     "resourceType": "TEQ2",
                    |     "questions": 5
                    |  },
                    |  "occurredOn":"2020-07-20 02:41:00.0"
                    |},
                    |{
                    |  "eventType": "LessonAssessmentRuleRemovedEvent",
                    |  "lessonId": 234567,
                    |  "lessonUuid": "995afcba-e214-4d3c-a84a-000000234567",
                    |  "stepUuid": "1ca1ab16-232c-42fc-9537-b9fe35fd207b",
                    |  "stepId": 1,
                    |  "rule": {
                    |     "id": "e07d4879-23be-4854-bd59-0a7647bd3f6b",
                    |     "poolId": "5f43c5f70e9eb20001907460",
                    |     "poolName": "pool 1",
                    |     "difficultyLevel": "EASY",
                    |     "resourceType": "TEQ1",
                    |     "questions": 4
                    |  },
                    |  "occurredOn":"2020-07-20 02:42:00.0"
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val deltaIwhDf = sinks
            .collect {
              case x: DeltaIwhSink => x
            }
            .map(_.output)
            .head

          val statuses = deltaIwhDf
            .orderBy(col(s"${LearningObjectiveStepInstanceEntity}_created_time"))
            .select(col(s"${LearningObjectiveStepInstanceEntity}_status"), col(s"${LearningObjectiveStepInstanceEntity}_attach_status"))
            .collect()
            .map(x => (x.getInt(0), x.getInt(1)))
            .toList

          assert(
            statuses == List((LessonAssociationInactiveStatusVal, LessonAssociationAttachedStatusVal),
                             (LessonAssociationInactiveStatusVal, LessonAssociationAttachedStatusVal),
                             (LessonAssociationActiveStatusVal, LessonAssociationDetachedStatusVal)))
        }
      )
    }
  }
}