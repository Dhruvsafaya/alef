package com.alefeducation.facts.weekly_goal

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.weekly_goal.base.WeeklyGoalCompletedStaging.ParquetWeeklyGoalStagingSink
import com.alefeducation.facts.weekly_goal.base.WeeklyGoalDelta.ParquetWeeklyGoalTransformedSource
import com.alefeducation.facts.weekly_goal.base.WeeklyGoalTransform._
import com.alefeducation.facts.weekly_goal.base._
import com.alefeducation.util.Constants.WeeklyGoalCompletedEvent
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

class WeeklyGoalSpec extends SparkSuite with Matchers{
  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns = Set(
    "fwg_id",
    "fwg_student_id",
    "fwg_type_id",
    "fwg_class_id",
    "fwg_action_status",
    "fwg_tenant_id",
    "fwg_star_earned",
    "fwg_created_time",
    "fwg_dw_created_time",
    "fwg_date_dw_id",
    "eventdate"
  )

  test("transform weekly goal completed event when GoalCompleted and StarEarned available in parquet and no data available in staging tables") {
    val weeklyGoalEvents =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalCreated",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "studentId": "546daba6-677a-4110-8843-54aaacb549b9",
        |  "classId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
        |  "goalId": "c38e0c92-68d0-4f96-a677-d169a0ae0d68",
        |  "occurredOn": "2021-06-23 04:33:24.921",
        |  "eventDateDw": "20210623"
        |},
        |{
        |  "eventType": "WeeklyGoalCreated",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "studentId": "546daba6-677a-4110-8843-54aaacb549b9",
        |  "classId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
        |  "goalId": "c38e0c92-68d0-4f96-a677-d169a0ae0d68",
        |  "occurredOn": "2021-06-23 04:33:24.920",
        |  "eventDateDw": "20210623"
        |},
        |{
        |  "eventType": "WeeklyGoalCompleted",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "studentId": "546daba6-677a-4110-8843-54aaacb549b9",
        |  "classId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
        |  "goalId": "c38e0c92-68d0-4f96-a677-d169a0ae0d68",
        |  "occurredOn": "2021-06-23 05:31:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin

    val starEarnedValue =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalStarEarned",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "studentId": "546daba6-677a-4110-8843-54aaacb549b9",
        |  "classId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
        |  "stars": 6,
        |  "academicYearId": "911bb80a-c012-4963-8ab4-23e9050183fd",
        |  "occurredOn": "2021-06-23 05:33:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val transformer = new WeeklyGoalTransform(sprk, service)
    val weeklyGoalDF = spark.read.json(Seq(weeklyGoalEvents).toDS())
    when(service.readOptional(ParquetWeeklyGoalSource, sprk)).thenReturn(Some(weeklyGoalDF))
    when(service.readOptional(ParquetWeeklyGoalStagingSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetWeeklyGoalStarEarnedStagingSource, sprk)).thenReturn(None)

    val starEarnedInputDF = spark.read.json(Seq(starEarnedValue).toDS())
    when(service.readOptional(WeeklyGoalStarEarnedDataSink.ParquetWeeklyGoalStarEarnedSource, spark)).thenReturn(Some(starEarnedInputDF))

    val df = transformer.transform().get.output
    val created = df.filter($"fwg_action_status" === 1)
    val completed = df.filter($"fwg_action_status" === 2)

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() === 2)
    assert[String](created, "fwg_id", "830a4d57-6e58-4554-ad71-f82e2cf9c6b3")
    assert[String](created, "fwg_student_id", "546daba6-677a-4110-8843-54aaacb549b9")
    assert[String](created, "fwg_type_id", "c38e0c92-68d0-4f96-a677-d169a0ae0d68")
    assert[String](created, "fwg_class_id", "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f")
    assert[Int](created, "fwg_action_status", 1)
    assert[String](created, "fwg_tenant_id", "tenant-id")
    assert[Int](created, "fwg_star_earned", -1)
    assert[String](created, "fwg_created_time", "2021-06-23 04:33:24.921")
    assert[String](created, "eventdate", "2021-06-23")

    assert[String](completed, "fwg_id", "830a4d57-6e58-4554-ad71-f82e2cf9c6b3")
    assert[String](completed, "fwg_student_id", "546daba6-677a-4110-8843-54aaacb549b9")
    assert[String](completed, "fwg_type_id", "c38e0c92-68d0-4f96-a677-d169a0ae0d68")
    assert[String](completed, "fwg_class_id", "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f")
    assert[Int](completed, "fwg_action_status", 2)
    assert[String](completed, "fwg_tenant_id", "tenant-id")
    assert[Int](completed, "fwg_star_earned", 6)
    assert[String](completed, "fwg_created_time", "2021-06-23 05:33:24.921")
    assert[String](completed, "eventdate", "2021-06-23")

  }

  test("transform weekly goal completed event when GoalCompleted is in staging and StarEarned in parquet") {
    val weeklyGoalStagingEvents =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalCompleted",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "studentId": "546daba6-677a-4110-8843-54aaacb549b9",
        |  "classId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
        |  "goalId": "c38e0c92-68d0-4f96-a677-d169a0ae0d68",
        |  "occurredOn": "2021-06-23 05:31:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin

    val starEarnedParquet =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalStarEarned",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "studentId": "546daba6-677a-4110-8843-54aaacb549b9",
        |  "classId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
        |  "stars": 6,
        |  "academicYearId": "911bb80a-c012-4963-8ab4-23e9050183fd",
        |  "occurredOn": "2021-06-23 05:33:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._
    val transformer = new WeeklyGoalTransform(sprk, service)
    val stagingWeeklyGoalCompleted = spark.read.json(Seq(weeklyGoalStagingEvents).toDS())
    val starEarnedParquetDF = spark.read.json(Seq(starEarnedParquet).toDS())

    when(service.readOptional(ParquetWeeklyGoalSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetWeeklyGoalStagingSource, sprk)).thenReturn(Some(stagingWeeklyGoalCompleted))
    when(service.readOptional(ParquetWeeklyGoalStarEarnedStagingSource, sprk)).thenReturn(None)
    when(service.readOptional(WeeklyGoalStarEarnedDataSink.ParquetWeeklyGoalStarEarnedSource, sprk)).thenReturn(Some(starEarnedParquetDF))

    val df = transformer.transform().get.output
    df.count() should be (1)
    val completed = df.filter($"fwg_action_status" === 2)

    assert[String](completed, "fwg_id", "830a4d57-6e58-4554-ad71-f82e2cf9c6b3")
    assert[String](completed, "fwg_student_id", "546daba6-677a-4110-8843-54aaacb549b9")
    assert[String](completed, "fwg_type_id", "c38e0c92-68d0-4f96-a677-d169a0ae0d68")
    assert[String](completed, "fwg_class_id", "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f")
    assert[Int](completed, "fwg_action_status", 2)
    assert[String](completed, "fwg_tenant_id", "tenant-id")
    assert[Int](completed, "fwg_star_earned", 6)
    assert[String](completed, "fwg_created_time", "2021-06-23 05:33:24.921")
    assert[String](completed, "eventdate", "2021-06-23")
  }

  test("transform weekly goal completed event when StarEarned is in staging and GoalCompleted in parquet ") {
    val goalCompletedParquetEvent =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalCompleted",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "studentId": "546daba6-677a-4110-8843-54aaacb549b9",
        |  "classId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
        |  "goalId": "c38e0c92-68d0-4f96-a677-d169a0ae0d68",
        |  "occurredOn": "2021-06-23 05:31:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin

    val starEarnedStagingEvent =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalStarEarned",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "studentId": "546daba6-677a-4110-8843-54aaacb549b9",
        |  "classId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
        |  "stars": 6,
        |  "academicYearId": "911bb80a-c012-4963-8ab4-23e9050183fd",
        |  "occurredOn": "2021-06-23 05:33:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._
    val transformer = new WeeklyGoalTransform(sprk, service)
    val weeklyGoalParquetDF = spark.read.json(Seq(goalCompletedParquetEvent).toDS())
    val starEarnedStagingDF = spark.read.json(Seq(starEarnedStagingEvent).toDS())

    when(service.readOptional(ParquetWeeklyGoalSource, sprk)).thenReturn(Some(weeklyGoalParquetDF))
    when(service.readOptional(ParquetWeeklyGoalStagingSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetWeeklyGoalStarEarnedStagingSource, sprk)).thenReturn(Some(starEarnedStagingDF))
    when(service.readOptional(WeeklyGoalStarEarnedDataSink.ParquetWeeklyGoalStarEarnedSource, sprk)).thenReturn(None)

    val df = transformer.transform().get.output
    df.count() should be (1)

    val completed = df.filter($"fwg_action_status" === 2)
    assert[String](completed, "fwg_id", "830a4d57-6e58-4554-ad71-f82e2cf9c6b3")
    assert[String](completed, "fwg_student_id", "546daba6-677a-4110-8843-54aaacb549b9")
    assert[String](completed, "fwg_type_id", "c38e0c92-68d0-4f96-a677-d169a0ae0d68")
    assert[String](completed, "fwg_class_id", "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f")
    assert[Int](completed, "fwg_action_status", 2)
    assert[String](completed, "fwg_tenant_id", "tenant-id")
    assert[Int](completed, "fwg_star_earned", 6)
    assert[String](completed, "fwg_created_time", "2021-06-23 05:33:24.921")
    assert[String](completed, "eventdate", "2021-06-23")

  }

  test("transform weekly goal expired event successfully") {
    val weeklyGoalEvents =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalCreated",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "studentId": "546daba6-677a-4110-8843-54aaacb549b9",
        |  "classId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
        |  "goalId": "c38e0c92-68d0-4f96-a677-d169a0ae0d68",
        |  "occurredOn": "2021-06-23 04:33:24.921",
        |  "eventDateDw": "20210623"
        |},
        |{
        |  "eventType": "WeeklyGoalExpired",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "studentId": "546daba6-677a-4110-8843-54aaacb549b9",
        |  "classId": "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f",
        |  "goalId": "c38e0c92-68d0-4f96-a677-d169a0ae0d68",
        |  "occurredOn": "2021-06-30 05:31:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val transformer = new WeeklyGoalTransform(sprk, service)
    val weeklyGoalDF = spark.read.json(Seq(weeklyGoalEvents).toDS())
    when(service.readOptional(ParquetWeeklyGoalSource, sprk)).thenReturn(Some(weeklyGoalDF))
    when(service.readOptional(ParquetWeeklyGoalStagingSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetWeeklyGoalStarEarnedStagingSource, sprk)).thenReturn(None)
    val starEarnedInputDF = spark.read.json(Seq("").toDS())
    when(service.readOptional(WeeklyGoalStarEarnedDataSink.ParquetWeeklyGoalStarEarnedSource, spark)).thenReturn(Some(starEarnedInputDF))

    val df = transformer.transform().get.output
    df.count() should be (2)

    val created = df.filter($"fwg_action_status" === 1)
    assert(df.columns.toSet === expectedColumns)
    assert[String](created, "fwg_id", "830a4d57-6e58-4554-ad71-f82e2cf9c6b3")
    assert[String](created, "fwg_student_id", "546daba6-677a-4110-8843-54aaacb549b9")
    assert[String](created, "fwg_type_id", "c38e0c92-68d0-4f96-a677-d169a0ae0d68")
    assert[String](created, "fwg_class_id", "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f")
    assert[Int](created, "fwg_action_status", 1)
    assert[String](created, "fwg_tenant_id", "tenant-id")
    assert[Int](created, "fwg_star_earned", -1)
    assert[String](created, "fwg_created_time", "2021-06-23 04:33:24.921")
    assert[String](created, "eventdate", "2021-06-23")

    val completed = df.filter($"fwg_action_status" === 3)
    assert[String](completed, "fwg_id", "830a4d57-6e58-4554-ad71-f82e2cf9c6b3")
    assert[String](completed, "fwg_student_id", "546daba6-677a-4110-8843-54aaacb549b9")
    assert[String](completed, "fwg_type_id", "c38e0c92-68d0-4f96-a677-d169a0ae0d68")
    assert[String](completed, "fwg_class_id", "c00d7463-8de6-4b68-9b3a-0f62b78b5a0f")
    assert[Int](completed, "fwg_action_status", 3)
    assert[String](completed, "fwg_tenant_id", "tenant-id")
    assert[Int](completed, "fwg_star_earned", -1)
    assert[String](completed, "fwg_created_time", "2021-06-30 05:31:24.921")
    assert[String](completed, "eventdate", "2021-06-30")
  }

  test("WeeklyGoalStaging should stage 0 records when events from processing and staging is moved to transformed folder in the current run") {
    val transformedWeeklyGoalEvents =
      """
        |[{
        | "fwg_id": "weekly-goal-id",
        | "fwg_student_id": "student-id",
        | "fwg_type_id": "goal-type-id",
        | "fwg_class_id": "class-id",
        | "fwg_action_status": 2,
        | "fwg_tenant_dw_id": "tenant-id",
        | "fwg_star_earned": 4,
        | "fwg_created_time": "2021-06-23 05:33:24.921",
        | "fwg_dw_created_time": "2021-06-23 05:33:24.921"
        |},{
        | "fwg_id": "weekly-goal-id-1",
        | "fwg_student_id": "student-id",
        | "fwg_type_id": "goal-type-id",
        | "fwg_class_id": "class-id",
        | "fwg_action_status": 2,
        | "fwg_tenant_dw_id": "tenant-id",
        | "fwg_star_earned": 4,
        | "fwg_created_time": "2021-06-23 05:33:24.921",
        | "fwg_dw_created_time": "2021-06-23 05:33:24.921"
        |}]
        """.stripMargin

    val stagingWeeklyGoalEvents =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalCompleted",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "weekly-goal-id",
        |  "studentId": "student-id",
        |  "classId": "class-id",
        |  "goalId": "goal-type-id",
        |  "occurredOn": "2021-06-23 04:33:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin

    val processingWeeklyGoalEvents =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalCompleted",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "weekly-goal-id-1",
        |  "studentId": "student-id",
        |  "classId": "class-id",
        |  "goalId": "goal-type-id",
        |  "occurredOn": "2021-06-23 04:33:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val stagingWeeklyGoalDF = spark.read.json(Seq(stagingWeeklyGoalEvents).toDS())
    val processingWeeklyGoalDF = spark.read.json(Seq(processingWeeklyGoalEvents).toDS())
    val transformedWeeklyGoalDF = spark.read.json(Seq(transformedWeeklyGoalEvents).toDS())
    val starEarnedInputDF = spark.read.json(Seq("").toDS())

    when(service.readOptional(ParquetWeeklyGoalSource, sprk)).thenReturn(Some(processingWeeklyGoalDF))
    when(service.readOptional(ParquetWeeklyGoalStagingSource, sprk)).thenReturn(Some(stagingWeeklyGoalDF))
    when(service.readOptional(ParquetWeeklyGoalTransformedSource, sprk)).thenReturn(Some(transformedWeeklyGoalDF))
    when(service.readOptional(ParquetWeeklyGoalStarEarnedSource, sprk)).thenReturn(Some(starEarnedInputDF))

    val transformer = new WeeklyGoalStaging(sprk, service, ParquetWeeklyGoalSource, WeeklyGoalCompletedEvent, ParquetWeeklyGoalStagingSource, ParquetWeeklyGoalStagingSink)

    val df = transformer.transform().get.output
    assert(df.count() === 0)
  }

  test("WeeklyGoalStaging should stage 0 records when events from processing only moved to transformed folder with staging being empty") {
    val transformedWeeklyGoalEvents =
      """
        |[{
        | "fwg_id": "weekly-goal-id-1",
        | "fwg_student_id": "student-id",
        | "fwg_type_id": "goal-type-id",
        | "fwg_class_id": "class-id",
        | "fwg_action_status": 2,
        | "fwg_tenant_dw_id": "tenant-id",
        | "fwg_star_earned": 4,
        | "fwg_created_time": "2021-06-23 05:33:24.921",
        | "fwg_dw_created_time": "2021-06-23 05:33:24.921"
        |}]
        """.stripMargin

    val processingWeeklyGoalEvents =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalCompleted",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "weekly-goal-id-1",
        |  "studentId": "student-id",
        |  "classId": "class-id",
        |  "goalId": "goal-type-id",
        |  "occurredOn": "2021-06-23 04:33:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val processingWeeklyGoalDF = spark.read.json(Seq(processingWeeklyGoalEvents).toDS())
    val transformedWeeklyGoalDF = spark.read.json(Seq(transformedWeeklyGoalEvents).toDS())
    val starEarnedInputDF = spark.read.json(Seq("").toDS())

    when(service.readOptional(ParquetWeeklyGoalSource, sprk)).thenReturn(Some(processingWeeklyGoalDF))
    when(service.readOptional(ParquetWeeklyGoalStagingSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetWeeklyGoalTransformedSource, sprk)).thenReturn(Some(transformedWeeklyGoalDF))
    when(service.readOptional(ParquetWeeklyGoalStarEarnedSource, sprk)).thenReturn(Some(starEarnedInputDF))

    val transformer = new WeeklyGoalStaging(sprk, service, ParquetWeeklyGoalSource, WeeklyGoalCompletedEvent, ParquetWeeklyGoalStagingSource, ParquetWeeklyGoalStagingSink)

    val df = transformer.transform().get.output
    assert(df.count() === 0)
  }

  test("WeeklyGoalStaging should stage 0 records when events from staging only moved to transformed folder with processing being empty") {
    val transformedWeeklyGoalEvents =
      """
        |[{
        | "fwg_id": "weekly-goal-id",
        | "fwg_student_id": "student-id",
        | "fwg_type_id": "goal-type-id",
        | "fwg_class_id": "class-id",
        | "fwg_action_status": 2,
        | "fwg_tenant_dw_id": "tenant-id",
        | "fwg_star_earned": 4,
        | "fwg_created_time": "2021-06-23 05:33:24.921",
        | "fwg_dw_created_time": "2021-06-23 05:33:24.921"
        |}]
        """.stripMargin

    val stagingWeeklyGoalEvents =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalCompleted",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "weekly-goal-id",
        |  "studentId": "student-id",
        |  "classId": "class-id",
        |  "goalId": "goal-type-id",
        |  "occurredOn": "2021-06-23 04:33:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val stagingWeeklyGoalDF = spark.read.json(Seq(stagingWeeklyGoalEvents).toDS())
    val transformedWeeklyGoalDF = spark.read.json(Seq(transformedWeeklyGoalEvents).toDS())
    val starEarnedInputDF = spark.read.json(Seq("").toDS())

    when(service.readOptional(ParquetWeeklyGoalSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetWeeklyGoalStagingSource, sprk)).thenReturn(Some(stagingWeeklyGoalDF))
    when(service.readOptional(ParquetWeeklyGoalTransformedSource, sprk)).thenReturn(Some(transformedWeeklyGoalDF))
    when(service.readOptional(ParquetWeeklyGoalStarEarnedSource, sprk)).thenReturn(Some(starEarnedInputDF))

    val transformer = new WeeklyGoalStaging(sprk, service, ParquetWeeklyGoalSource, WeeklyGoalCompletedEvent, ParquetWeeklyGoalStagingSource, ParquetWeeklyGoalStagingSink)

    val df = transformer.transform().get.output
    assert(df.count() === 0)
  }

  test("WeeklyGoalStaging should keep the staging records in staging until it is moved to transformed folder") {
    val transformed =
      """
        |[{
        | "fwg_id": "weekly-goal-id",
        | "fwg_student_id": "student-id",
        | "fwg_type_id": "goal-type-id",
        | "fwg_class_id": "class-id",
        | "fwg_action_status": 2,
        | "fwg_tenant_dw_id": "tenant-id",
        | "fwg_star_earned": 4,
        | "fwg_created_time": "2021-06-23 05:33:24.921",
        | "fwg_dw_created_time": "2021-06-23 05:33:24.921"
        |}]
        """.stripMargin

    val staging =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalCompleted",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "weekly-goal-id-2",
        |  "studentId": "student-id",
        |  "classId": "class-id",
        |  "goalId": "goal-type-id",
        |  "occurredOn": "2021-06-23 04:33:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin

    val processing =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalCompleted",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "weekly-goal-id",
        |  "studentId": "student-id",
        |  "classId": "class-id",
        |  "goalId": "goal-type-id",
        |  "occurredOn": "2021-06-23 04:33:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val transformer = new WeeklyGoalStaging(sprk, service, ParquetWeeklyGoalSource, WeeklyGoalCompletedEvent, ParquetWeeklyGoalStagingSource, ParquetWeeklyGoalStagingSink)

    val stagingDF = spark.read.json(Seq(staging).toDS())
    val inputDF = spark.read.json(Seq(processing).toDS())
    val transformedDF = spark.read.json(Seq(transformed).toDS())

    when(service.readOptional(ParquetWeeklyGoalSource, sprk)).thenReturn(Some(inputDF))
    when(service.readOptional(ParquetWeeklyGoalStagingSource, sprk)).thenReturn(Some(stagingDF))
    when(service.readOptional(ParquetWeeklyGoalTransformedSource, sprk)).thenReturn(Some(transformedDF))

    val starEarnedInputDF = spark.read.json(Seq("").toDS())
    when(service.readOptional(ParquetWeeklyGoalStarEarnedSource, sprk)).thenReturn(Some(starEarnedInputDF))

    val df = transformer.transform().get.output
    assert(df.count() === 1)
    assert[String](df, "eventType", "WeeklyGoalCompleted")
    assert[String](df, "weeklyGoalId", "weekly-goal-id-2")
  }

  test("WeeklyGoalStaging should keep the record from processing in stage if it doesn't get moved to transformed folder when staging is empty") {
    val transformed =
      """
        |[{
        | "fwg_id": "weekly-goal-id",
        | "fwg_student_id": "student-id",
        | "fwg_type_id": "goal-type-id",
        | "fwg_class_id": "class-id",
        | "fwg_action_status": 2,
        | "fwg_tenant_dw_id": "tenant-id",
        | "fwg_star_earned": 4,
        | "fwg_created_time": "2021-06-23 05:33:24.921",
        | "fwg_dw_created_time": "2021-06-23 05:33:24.921"
        |}]
        """.stripMargin

    val processing =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalCompleted",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "weekly-goal-id-1",
        |  "studentId": "student-id",
        |  "classId": "class-id",
        |  "goalId": "goal-type-id",
        |  "occurredOn": "2021-06-23 04:33:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val transformer = new WeeklyGoalStaging(sprk, service, ParquetWeeklyGoalSource, WeeklyGoalCompletedEvent, ParquetWeeklyGoalStagingSource, ParquetWeeklyGoalStagingSink)

    val inputDF = spark.read.json(Seq(processing).toDS())
    val transformedDF = spark.read.json(Seq(transformed).toDS())
    val starEarnedInputDF = spark.read.json(Seq("").toDS())

    when(service.readOptional(ParquetWeeklyGoalSource, sprk)).thenReturn(Some(inputDF))
    when(service.readOptional(ParquetWeeklyGoalStagingSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetWeeklyGoalTransformedSource, sprk)).thenReturn(Some(transformedDF))
    when(service.readOptional(ParquetWeeklyGoalStarEarnedSource, sprk)).thenReturn(Some(starEarnedInputDF))

    val df = transformer.transform().get.output

    assert(df.count() === 1)
    assert[String](df, "eventType", "WeeklyGoalCompleted")
    assert[String](df, "weeklyGoalId", "weekly-goal-id-1")
  }

  test("WeeklyGoalStaging should stage only Goal Completed event when Goal Created event moves to transformed but not Goal Completed event") {
    val transformed =
      """
        |[{
        | "fwg_id": "weekly-goal-id",
        | "fwg_student_id": "student-id",
        | "fwg_type_id": "goal-type-id",
        | "fwg_class_id": "class-id",
        | "fwg_action_status": 1,
        | "fwg_tenant_dw_id": "tenant-id",
        | "fwg_star_earned": -1,
        | "fwg_created_time": "2021-06-23 05:33:24.921",
        | "fwg_dw_created_time": "2021-06-23 05:33:24.921"
        |}]
        """.stripMargin

    val processing =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalCreated",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "weekly-goal-id",
        |  "studentId": "student-id",
        |  "classId": "class-id",
        |  "goalId": "goal-type-id",
        |  "occurredOn": "2021-06-23 04:33:21.921",
        |  "eventDateDw": "20210623"
        |},
        |{
        |  "eventType": "WeeklyGoalCompleted",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "weekly-goal-id",
        |  "studentId": "student-id",
        |  "classId": "class-id",
        |  "goalId": "goal-type-id",
        |  "occurredOn": "2021-06-23 04:33:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val transformer = new WeeklyGoalStaging(sprk, service, ParquetWeeklyGoalSource, WeeklyGoalCompletedEvent, ParquetWeeklyGoalStagingSource, ParquetWeeklyGoalStagingSink)

    val inputDF = spark.read.json(Seq(processing).toDS())
    val transformedDF = spark.read.json(Seq(transformed).toDS())
    val starEarnedInputDF = spark.read.json(Seq("").toDS())

    when(service.readOptional(ParquetWeeklyGoalSource, sprk)).thenReturn(Some(inputDF))
    when(service.readOptional(ParquetWeeklyGoalStagingSource, sprk)).thenReturn(None)
    when(service.readOptional(ParquetWeeklyGoalTransformedSource, sprk)).thenReturn(Some(transformedDF))
    when(service.readOptional(ParquetWeeklyGoalStarEarnedSource, sprk)).thenReturn(None)

    val df = transformer.transform().get.output

    assert(df.count() === 1)
    assert[String](df, "eventType", "WeeklyGoalCompleted")
    assert[String](df, "weeklyGoalId", "weekly-goal-id")
  }

  test("WeeklyGoalStaging should keep the record from processing in stage with the existing records in staging if it doesn't get moved to transformed folder with") {
    val transformed =
      """
        |[{
        | "fwg_id": "weekly-goal-id",
        | "fwg_student_id": "student-id",
        | "fwg_type_id": "goal-type-id",
        | "fwg_class_id": "class-id",
        | "fwg_action_status": 2,
        | "fwg_tenant_dw_id": "tenant-id",
        | "fwg_star_earned": 4,
        | "fwg_created_time": "2021-06-23 05:33:24.921",
        | "fwg_dw_created_time": "2021-06-23 05:33:24.921"
        |}]
        """.stripMargin

    val staging =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalCompleted",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "weekly-goal-id-2",
        |  "studentId": "student-id",
        |  "classId": "class-id",
        |  "goalId": "goal-type-id",
        |  "occurredOn": "2021-06-23 04:33:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin

    val processing =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalCompleted",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "weekly-goal-id-1",
        |  "studentId": "student-id",
        |  "classId": "class-id",
        |  "goalId": "goal-type-id",
        |  "occurredOn": "2021-06-23 04:33:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val transformer = new WeeklyGoalStaging(sprk, service, ParquetWeeklyGoalSource, WeeklyGoalCompletedEvent, ParquetWeeklyGoalStagingSource, ParquetWeeklyGoalStagingSink)

    val stagingDF = spark.read.json(Seq(staging).toDS())
    val inputDF = spark.read.json(Seq(processing).toDS())
    val transformedDF = spark.read.json(Seq(transformed).toDS())
    val starEarnedInputDF = spark.read.json(Seq("").toDS())

    when(service.readOptional(ParquetWeeklyGoalSource, sprk)).thenReturn(Some(inputDF))
    when(service.readOptional(ParquetWeeklyGoalStagingSource, sprk)).thenReturn(Some(stagingDF))
    when(service.readOptional(ParquetWeeklyGoalTransformedSource, sprk)).thenReturn(Some(transformedDF))
    when(service.readOptional(ParquetWeeklyGoalStarEarnedSource, sprk)).thenReturn(Some(starEarnedInputDF))

    val df = transformer.transform().get.output

    assert(df.count() === 2)

    val stagingGoalIds = df.collect().map(_.getAs[String]("weeklyGoalId"))
    stagingGoalIds should contain ("weekly-goal-id-1")
    stagingGoalIds should contain ("weekly-goal-id-2")
  }

}
