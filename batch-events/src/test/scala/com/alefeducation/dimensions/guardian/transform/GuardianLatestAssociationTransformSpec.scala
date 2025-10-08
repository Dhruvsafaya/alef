package com.alefeducation.dimensions.guardian.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationTransform.GuardianAssociationTransformSink
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class GuardianLatestAssociationTransformSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("guardian latest association should transform") {
    val associationValue =
      """
        |[
        |{
        |   "guardian_id": "guardian-id-2",
        |   "guardian_invitation_status": 2,
        |   "guardian_status": 1,
        |   "student_id": "studentId1",
        |   "guardian_created_time": "1970-07-15 03:55:00.0",
        |   "guardian_dw_created_time": "1970-07-15 04:55:00.0",
        |   "guardian_updated_time": null,
        |   "guardian_deleted_time": null,
        |   "guardian_dw_updated_time": null,
        |   "guardian_active_until": null
        |},
        |{
        |   "guardian_id": "guardian-id-2",
        |   "guardian_invitation_status": 2,
        |   "guardian_status": 1,
        |   "student_id": "studentId2",
        |   "guardian_created_time": "1970-07-15 05:11:00.0",
        |   "guardian_dw_created_time": "1970-07-15 06:00:00.0",
        |   "guardian_updated_time": null,
        |   "guardian_deleted_time": null,
        |   "guardian_dw_updated_time": null,
        |   "guardian_active_until": null
        |},
        |{
        |   "guardian_id": "guardian-id-3",
        |   "guardian_invitation_status": 2,
        |   "guardian_status": 1,
        |   "student_id": "studentId2",
        |   "guardian_created_time": "1970-07-15 05:11:00.0",
        |   "guardian_dw_created_time": "1970-07-15 06:00:00.0",
        |   "guardian_updated_time": null,
        |   "guardian_deleted_time": null,
        |   "guardian_dw_updated_time": null,
        |   "guardian_active_until": null
        |}
        |]
        """.stripMargin

    val expectedColumns = Set(
      "guardian_id",
      "guardian_invitation_status",
      "guardian_status",
      "student_id",
      "guardian_created_time",
      "guardian_dw_created_time",
      "guardian_updated_time",
      "guardian_deleted_time",
      "guardian_dw_updated_time",
      "guardian_active_until"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GuardianLatestAssociationTransform(sprk, service)

    val associationInput = spark.read.json(Seq(associationValue).toDS())
    when(service.readOptional(GuardianAssociationTransformSink, sprk)).thenReturn(Some(associationInput))

    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)

    val guardian2 = df.filter($"guardian_id" === "guardian-id-2")
    assert[Int](guardian2, "guardian_invitation_status", 2)
    assert[Int](guardian2, "guardian_status", 2)
    assert[String](guardian2, "student_id", "studentId2")
    assert[String](guardian2, "guardian_created_time", "1970-07-15 05:11:00.0")
    assert[String](guardian2, "guardian_active_until", "1970-07-15 05:11:00.0")

    val guardian3 = df.filter($"guardian_id" === "guardian-id-3")
    assert[Int](guardian2, "guardian_invitation_status", 2)
    assert[Int](guardian2, "guardian_status", 2)
    assert[String](guardian2, "student_id", "studentId2")
    assert[String](guardian2, "guardian_created_time", "1970-07-15 05:11:00.0")
    assert[String](guardian2, "guardian_active_until", "1970-07-15 05:11:00.0")
  }
}
