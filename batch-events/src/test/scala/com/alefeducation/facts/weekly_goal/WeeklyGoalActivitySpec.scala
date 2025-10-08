package com.alefeducation.facts.weekly_goal

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.weekly_goal.activities.WeeklyGoalActivityTransform.ParquetWeeklyGoalActivitySource
import com.alefeducation.facts.weekly_goal.activities._
import com.alefeducation.util.{ParquetBatchWriter, delta, redshift}
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class WeeklyGoalActivitySpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns = Set(
    "fwga_weekly_goal_id",
    "fwga_completed_activity_id",
    "fwga_weekly_goal_status",
    "fwga_created_time",
    "fwga_dw_created_time",
    "fwga_date_dw_id",
    "eventdate"
  )

  test("transform weekly goal activity event successfully") {

    val value =
      """
        |[{
        |  "eventType": "WeeklyGoalCreated",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "completedActivity": "f82e2cf9-c6b3-830a-4d57-6e58-4554ad71",
        |  "status": "Ongoing",
        |  "occurredOn": "2021-06-23 04:33:24.921",
        |  "eventDateDw": "20210623"
        |},
        |{
        |  "eventType": "WeeklyGoalCreated",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "completedActivity": "f82e2cf9-c6b3-830a-4d57-6e58-4554ad71",
        |  "status": "Ongoing",
        |  "occurredOn": "2021-06-23 04:33:24.911",
        |  "eventDateDw": "20210623"
        |},
        |{
        |  "eventType": "WeeklyGoalCreated",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "completedActivity": "11111111-c6b3-830a-4d57-6e58-4554ad71",
        |  "status": "Completed",
        |  "occurredOn": "2021-06-23 04:35:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val transformer = new WeeklyGoalActivityTransform(sprk, service)
    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional(ParquetWeeklyGoalActivitySource, sprk)).thenReturn(Some(inputDF))

    val df = transformer.transform().get.output
    val ongoing = df.filter($"fwga_weekly_goal_status" === 1)
    val completed = df.filter($"fwga_weekly_goal_status" === 2)

    assert(df.columns.toSet === expectedColumns)
    assertRow[String](ongoing.first, "fwga_weekly_goal_id", "830a4d57-6e58-4554-ad71-f82e2cf9c6b3")
    assertTimestamp(ongoing, "fwga_created_time", "2021-06-23 04:33:24.921")
    assertRow[String](ongoing.first, "fwga_completed_activity_id", "f82e2cf9-c6b3-830a-4d57-6e58-4554ad71")
    assertRow[Int](ongoing.first, "fwga_weekly_goal_status", 1)
    assertRow[String](ongoing.first, "eventdate", "2021-06-23")

    assertRow[String](completed.first, "fwga_weekly_goal_id", "830a4d57-6e58-4554-ad71-f82e2cf9c6b3")
    assertRow[String](completed.first, "fwga_completed_activity_id", "11111111-c6b3-830a-4d57-6e58-4554ad71")
    assertTimestamp(completed, "fwga_created_time", "2021-06-23 04:35:24.921")
    assertRow[Int](completed.first, "fwga_weekly_goal_status", 2)
    assertRow[String](completed.first, "eventdate", "2021-06-23")
  }

  test("delta sink for weekly goal activity event successfully") {

    val value =
      """
        |[
        |{
        |  "fwga_weekly_goal_id": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "fwga_completed_activity_id": "546daba6-677a-4110-8843-54aaacb549b9",
        |  "fwga_weekly_goal_status": 1,
        |  "fwga_created_time": "2021-06-23 04:33:24.921",
        |  "fwga_dw_created_time": "2021-06-23 04:33:24.921",
        |  "fwga_date_dw_id": 20210623,
        |  "eventdate": "2021-06-23"
        |},
        |{
        |  "fwga_weekly_goal_id": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "fwga_completed_activity_id": "546daba6-677a-4110-8843-54aaacb549b9",
        |  "fwga_weekly_goal_status": 2,
        |  "fwga_created_time": "2021-06-23 05:33:24.921",
        |  "fwga_dw_created_time": "2021-06-23 05:33:24.921",
        |  "fwga_date_dw_id": 20210623,
        |  "eventdate": "2021-06-23"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val writer = new delta.InsertIfNotExists(sprk, service, "delta-weekly-goal-activity")
    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional("parquet-weekly-goal-activity-transformed-source", sprk)).thenReturn(Some(inputDF))

    val df = writer.write().get.output
    val created = df.filter($"fwga_weekly_goal_status" === 1).first()
    val completed = df.filter($"fwga_weekly_goal_status" === 2).first()

    assert(df.columns.toSet === expectedColumns)
    assertRow[String](created, "fwga_weekly_goal_id", "830a4d57-6e58-4554-ad71-f82e2cf9c6b3")
    assertRow[Int](created, "fwga_weekly_goal_status", 1)
    assertRow[String](created, "fwga_created_time", "2021-06-23 04:33:24.921")
    assertRow[String](created, "eventdate", "2021-06-23")

    assertRow[String](completed, "fwga_weekly_goal_id", "830a4d57-6e58-4554-ad71-f82e2cf9c6b3")
    assertRow[Int](completed, "fwga_weekly_goal_status", 2)
    assertRow[String](completed, "fwga_created_time", "2021-06-23 05:33:24.921")
    assertRow[String](completed, "eventdate", "2021-06-23")
  }

  test("redshift sink for weekly goal activity event successfully") {

    val value =
      """
        |[
        |{
        |  "fwga_weekly_goal_id": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "fwga_completed_activity_id": "546daba6-677a-4110-8843-54aaacb549b9",
        |  "fwga_weekly_goal_status": 1,
        |  "fwga_created_time": "2021-06-23 04:33:24.921",
        |  "fwga_dw_created_time": "2021-06-23 04:33:24.921",
        |  "fwga_date_dw_id": 20210623,
        |  "eventdate": "2021-06-23"
        |},
        |{
        |  "fwga_weekly_goal_id": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "fwga_completed_activity_id": "546daba6-677a-4110-8843-54aaacb549b9",
        |  "fwga_weekly_goal_status": 2,
        |  "fwga_created_time": "2021-06-23 05:33:24.921",
        |  "fwga_dw_created_time": "2021-06-23 05:33:24.921",
        |  "fwga_date_dw_id": 20210623,
        |  "eventdate": "2021-06-23"
        |}]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._
    val writer = new redshift.InsertIfNotExists(sprk, service, "redshift-weekly-goal-activity")
    val inputDF = spark.read.json(Seq(value).toDS())
    when(service.readOptional("parquet-weekly-goal-activity-transformed-source", sprk)).thenReturn(Some(inputDF))

    val df = writer.write().get.output
    val created = df.filter($"fwga_weekly_goal_status" === 1).first()
    val completed = df.filter($"fwga_weekly_goal_status" === 2).first()

    assert(df.columns.toSet === expectedColumns - "eventdate")
    assertRow[String](created, "fwga_weekly_goal_id", "830a4d57-6e58-4554-ad71-f82e2cf9c6b3")
    assertRow[Int](created, "fwga_weekly_goal_status", 1)
    assertRow[String](created, "fwga_created_time", "2021-06-23 04:33:24.921")
    assertRow[String](completed, "fwga_weekly_goal_id", "830a4d57-6e58-4554-ad71-f82e2cf9c6b3")
    assertRow[Int](completed, "fwga_weekly_goal_status", 2)
    assertRow[String](completed, "fwga_created_time", "2021-06-23 05:33:24.921")
  }

  test("parquet sink for weekly goal activity event successfully") {

    val starEarnedValue =
      """
        |[
        |{
        |  "eventType": "WeeklyGoalProgress",
        |  "tenantId": "tenant-id",
        |  "weeklyGoalId": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
        |  "completedActivity": "2cf9c6b3-6e58-4554-ad71-f82e2cf9c6b3",
        |  "status": "Ongoing",
        |  "occurredOn": "2021-06-23 05:33:24.921",
        |  "eventDateDw": "20210623"
        |}]
        """.stripMargin
    val sprk = spark
    import sprk.implicits._
    val writer = new ParquetBatchWriter(sprk, service, "parquet-weekly-goal-activity-source", "parquet-weekly-goal-activity-sink")
    val starEarnedInputDF = spark.read.json(Seq(starEarnedValue).toDS())
    when(service.readOptional("parquet-weekly-goal-activity-source", sprk)).thenReturn(Some(starEarnedInputDF))
    val expectedColumns = Set(
      "eventType",
      "tenantId",
      "weeklyGoalId",
      "completedActivity",
      "status",
      "occurredOn",
      "eventDateDw",
      "eventdate"
    )
    val df = writer.write().get.output
    assert(df.columns.toSet === expectedColumns)
  }


}
