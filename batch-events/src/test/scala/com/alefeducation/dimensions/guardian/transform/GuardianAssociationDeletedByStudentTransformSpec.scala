package com.alefeducation.dimensions.guardian.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationCommonTransform.GuardianAssociationCommonTransformSink
import com.alefeducation.models.GuardianModel.{DimGuardian, RelGuardian}
import com.alefeducation.models.StudentModel.StudentDim
import com.alefeducation.util.Helpers.{
  ParquetGuardianAssociationsSource,
  RedshiftDimStudentSink,
  RedshiftGuardianDimSource,
  RedshiftGuardianRelSink
}
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class GuardianAssociationDeletedByStudentTransformSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("guardian association deleted by student should transform") {
    val associationValue =
      """
        |[
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"GuardianAssociationsUpdated",
        |   "createdOn":12345,
        |   "studentId":"studentId1",
        |   "guardians":[{"id":"guardian-id-2"}],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-15 02:40:00.0"
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"GuardianAssociationsUpdated",
        |   "createdOn":12346,
        |   "studentId":"studentId1",
        |   "guardians":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-15 03:55:00.0"
        |}
        |]
        """.stripMargin

    val associationCommonValue =
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

    val dimRedshiftValue =
      """
        |[
        |{
        |   "guardian_dw_id": 111,
        |   "guardian_id": "guardian-id-2",
        |   "guardian_status": 1,
        |   "guardian_invitation_status": 2,
        |   "guardian_student_dw_id": 1,
        |   "guardian_active_until": null,
        |   "guardian_created_time": "1970-07-14 02:41:00.0"
        |}
        |]
        |""".stripMargin

    val studentRedshiftValue =
      """
        |[{
        |   "student_dw_id": 1,
        |   "student_id": "studentId1",
        |   "student_status": 1,
        |   "student_created_time": "1970-07-13 02:41:00.0",
        |   "student_active_until": null
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

    val transformer = new GuardianAssociationDeletedByStudentTransform(sprk, service)

    val associationInput = spark.read.json(Seq(associationValue).toDS())
    when(service.readOptional(ParquetGuardianAssociationsSource, sprk)).thenReturn(Some(associationInput))

    val associationCommonInput = spark.read.json(Seq(associationCommonValue).toDS())
    when(service.readOptional(GuardianAssociationCommonTransformSink, sprk)).thenReturn(Some(associationCommonInput))

    val relRedshiftInput = Seq.empty[RelGuardian].toDF()
    when(service.readFromRedshift[RelGuardian](RedshiftGuardianRelSink)).thenReturn(relRedshiftInput)

    val dimRedshiftInput = spark.read.json(Seq(dimRedshiftValue).toDS())
    when(service.readFromRedshift[DimGuardian](RedshiftGuardianDimSource)).thenReturn(dimRedshiftInput)

    val studentRedshiftInput = spark.read.json(Seq(studentRedshiftValue).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(studentRedshiftInput)

    when(service.getStartIdUpdateStatus("dim_guardian")).thenReturn(1)

    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)

    assert[String](df, "guardian_id", "guardian-id-2")
    assert[Int](df, "guardian_invitation_status", 2)
    assert[Int](df, "guardian_status", 4)
    assert[String](df, "student_id", "studentId1")
    assert[String](df, "guardian_created_time", "1970-07-15 03:55:00.0")
    assert[String](df, "guardian_active_until", null)
    assert[Int](df, "rel_guardian_dw_id", 1)
  }
}
