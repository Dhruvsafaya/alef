package com.alefeducation.facts.guardian_joint_activity

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.guardian_joint_activity.GuardianJointActivityTransform.{GuardianJointActivityCompletedSource, GuardianJointActivityPendingSource, GuardianJointActivityRatedSource, GuardianJointActivityStartedSource}
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class GuardianJointActivityTransformSpec extends SparkSuite {

  val sprk: SparkSession = spark
  val service = mock[SparkBatchService]

  val expectedColumns: Set[String] = Set(
    "fgja_dw_created_time",
    "fgja_student_id",
    "fgja_attempt",
    "eventdate",
    "fgja_date_dw_id",
    "fgja_class_id",
    "fgja_k12_grade",
    "fgja_created_time",
    "fgja_school_id",
    "fgja_state",
    "fgja_tenant_id",
    "fgja_guardian_id",
    "fgja_rating",
    "fgja_pathway_level_id",
    "fgja_pathway_id"
  )

  test("transform GuardianJointActivityPendingEvent successfully") {
    val GuardianJointActivityPendingEvent = """
            |[
            |{
            |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
            |  "eventType": "GuardianJointActivityPendingEvent",
            |  "loadtime": "2023-08-02 13:23:49.545",
            |  "guardianIds": ["76a17f37-7039-4547-91c0-9da819be50db","86a17f37-7039-4547-91c0-9da819be50db"],
            |  "pathwayId": "0d473d2c-e35e-4d4c-a373-2ac2c735b806",
            |  "pathwayLevelId": "0adaa230-afbd-4c75-9f22-3edd468a5129",
            |  "classId": "51ded7eb-624d-43d2-8203-50d5248e9d52",
            |  "studentId": "5fde2c96-7a34-47a3-a166-f8813e4a51fb",
            |  "schoolId": "db117ed7-30cf-4ea3-baf5-385b66109cb6",
            |  "grade": 6,
            |  "occurredOn": "2023-08-03 08:59:07.401",
            |  "eventDateDw": 20230803
            |}
            |]
        """.stripMargin

    val GuardianJointActivityStartedEvent =
      """
        |[
        |{
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "eventType": "GuardianJointActivityStartedEvent",
        |  "loadtime": "2023-08-02 13:23:49.545",
        |  "pathwayId": "0d473d2c-e35e-4d4c-a373-2ac2c735b806",
        |  "pathwayLevelId": "0adaa230-afbd-4c75-9f22-3edd468a5129",
        |  "classId": "51ded7eb-624d-43d2-8203-50d5248e9d52",
        |  "studentId": "5fde2c96-7a34-47a3-a166-f8813e4a51fb",
        |  "schoolId": "db117ed7-30cf-4ea3-baf5-385b66109cb6",
        |  "startedByGuardianId": "76a17f37-7039-4547-91c0-9da819be50db",
        |  "grade": 6,
        |  "attempt": 1,
        |  "occurredOn": "2023-08-03 08:59:07.401",
        |  "eventDateDw": 20230803
        |}
        |]
    """.stripMargin

    val GuardianJointActivityCompletedEvent =
      """
        |[
        |{
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "eventType": "GuardianJointActivityCompletedEvent",
        |  "loadtime": "2023-08-02 13:23:49.545",
        |  "pathwayId": "0d473d2c-e35e-4d4c-a373-2ac2c735b806",
        |  "pathwayLevelId": "0adaa230-afbd-4c75-9f22-3edd468a5129",
        |  "classId": "51ded7eb-624d-43d2-8203-50d5248e9d52",
        |  "studentId": "5fde2c96-7a34-47a3-a166-f8813e4a51fb",
        |  "schoolId": "db117ed7-30cf-4ea3-baf5-385b66109cb6",
        |  "completedByGuardianId": "76a17f37-7039-4547-91c0-9da819be50db",
        |  "grade": 6,
        |  "attempt": 1,
        |  "assignedOn": 1624426404921,
        |  "startedOn": 1624426404921,
        |  "occurredOn": "2023-08-03 08:59:07.401",
        |  "eventDateDw": 20230803
        |}
        |]
        """.stripMargin

    val GuardianJointActivityRatedEvent =
      """
        |[
        |{
        |  "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "eventType": "GuardianJointActivityRatedEvent",
        |  "loadtime": "2023-08-02 13:23:49.545",
        |  "pathwayId": "0d473d2c-e35e-4d4c-a373-2ac2c735b806",
        |  "pathwayLevelId": "0adaa230-afbd-4c75-9f22-3edd468a5129",
        |  "classId": "51ded7eb-624d-43d2-8203-50d5248e9d52",
        |  "studentId": "5fde2c96-7a34-47a3-a166-f8813e4a51fb",
        |  "schoolId": "db117ed7-30cf-4ea3-baf5-385b66109cb6",
        |  "guardianId": "76a17f37-7039-4547-91c0-9da819be50db",
        |  "grade": 6,
        |  "attempt": 1,
        |  "rating": 9,
        |  "occurredOn": "2023-08-03 08:59:07.401",
        |  "eventDateDw": 20230803
        |}
        |]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val GuardianJointActivityTransformer = new GuardianJointActivityTransform(sprk, service)

    val pendingDF = spark.read.json(Seq(GuardianJointActivityPendingEvent).toDS())
    val startedDF = spark.read.json(Seq(GuardianJointActivityStartedEvent).toDS())
    val completedDF = spark.read.json(Seq(GuardianJointActivityCompletedEvent).toDS())
    val ratedDF = spark.read.json(Seq(GuardianJointActivityRatedEvent).toDS())

    when(service.readOptional(GuardianJointActivityPendingSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(pendingDF))
    when(service.readOptional(GuardianJointActivityStartedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(startedDF))
    when(service.readOptional(GuardianJointActivityCompletedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(completedDF))
    when(service.readOptional(GuardianJointActivityRatedSource, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(ratedDF))
    val allStatesDF = GuardianJointActivityTransformer.transform().get.output

    println(allStatesDF.columns.toSet)
    assert(allStatesDF.count() === 5)
    assert(allStatesDF.columns.toSet === expectedColumns)

    val pendingGuardian = allStatesDF
      .filter($"fgja_guardian_id" === "76a17f37-7039-4547-91c0-9da819be50db")
      .filter($"fgja_attempt".isNull)

    assert[String](pendingGuardian, "fgja_student_id", "5fde2c96-7a34-47a3-a166-f8813e4a51fb")
    assert[String](pendingGuardian, "fgja_date_dw_id", "20230803")
    assert[String](pendingGuardian, "fgja_class_id", "51ded7eb-624d-43d2-8203-50d5248e9d52")
    assert[Int](pendingGuardian, "fgja_k12_grade", 6)
    assert[String](pendingGuardian, "fgja_created_time", "2023-08-03 08:59:07.401")
    assert[String](pendingGuardian, "fgja_school_id", "db117ed7-30cf-4ea3-baf5-385b66109cb6")
    assert[Int](pendingGuardian, "fgja_state", 1)
    assert[String](pendingGuardian, "fgja_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](pendingGuardian, "fgja_rating", null)
    assert[String](pendingGuardian, "fgja_pathway_level_id", "0adaa230-afbd-4c75-9f22-3edd468a5129")
    assert[String](pendingGuardian, "fgja_pathway_id", "0d473d2c-e35e-4d4c-a373-2ac2c735b806")

  }
}

