package com.alefeducation.dimensions.guardian.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationCommonTransform.GuardianAssociationCommonTransformSink
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class GuardianAssociationTransformSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("guardian association should transform") {
    val associationValue =
      """
        |[
        |{
        |   "guardianId":"guardian-id-2",
        |   "studentId":"studentId1",
        |   "occurredOn": "1970-07-15 03:55:00.0",
        |   "status": 1,
        |   "invitationStatus": 2
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
      "guardian_active_until",
      "rel_guardian_dw_id",
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GuardianAssociationTransform(sprk, service)

    val associationInput = spark.read.json(Seq(associationValue).toDS())
    when(service.readOptional(GuardianAssociationCommonTransformSink, sprk)).thenReturn(Some(associationInput))

    when(service.getStartIdUpdateStatus("dim_guardian")).thenReturn(1)

    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    assert[String](df, "guardian_id", "guardian-id-2")
    assert[Int](df, "guardian_invitation_status", 2)
    assert[Int](df, "guardian_status", 1)
    assert[String](df, "student_id", "studentId1")
    assert[String](df, "guardian_created_time", "1970-07-15 03:55:00.0")
    assert[String](df, "guardian_active_until", null)
    assert[Int](df, "rel_guardian_dw_id", 1)
  }
}
