package com.alefeducation.dimensions.guardian.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.guardian.transform.GuardianAssociationCommonTransform.GuardianAssociationCommonTransformSink
import com.alefeducation.dimensions.guardian.transform.GuardianRegistrationCommonTransform.GuardianRegistrationCommonTransformSink
import com.alefeducation.models.GuardianModel.{DimGuardian, RelGuardian}
import com.alefeducation.models.StudentModel.StudentDim
import com.alefeducation.util.Helpers.{ParquetGuardianDeletedSource, RedshiftDimStudentSink, RedshiftGuardianDimSource, RedshiftGuardianRelSink}
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class GuardianAssociationDeletedTransformSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("guardian association deleted should transform") {
    val deletedValue =
      """
        |[
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"GuardianDeleted",
        |   "events": [
        |     {"createdOn":111,"uuid":"guardian-id-1"},
        |     {"createdOn":222,"uuid":"guardian-id-2"},
        |     {"createdOn":333,"uuid":"guardian-id-3"}
        |   ],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181810",
        |   "occurredOn": "1970-07-14 02:40:00.0"
        |}
        |]
        |""".stripMargin

    val relRedshiftValue =
      """
        |[
        |{
        |   "guardian_id": "guardian-id-1",
        |   "guardian_status": 2,
        |   "guardian_invitation_status": 2,
        |   "student_id": "studentId3",
        |   "guardian_active_until": "1970-07-14 02:41:00.0",
        |   "guardian_created_time": "1970-07-14 00:00:00.0"
        |},
        |{
        |   "guardian_id": "guardian-id-1",
        |   "guardian_status": 1,
        |   "guardian_invitation_status": 2,
        |   "student_id": "studentId1",
        |   "guardian_active_until": null,
        |   "guardian_created_time": "1970-07-14 02:41:00.0"
        |},
        |{
        |   "guardian_id": "guardian-id-4",
        |   "guardian_status": 1,
        |   "guardian_invitation_status": 1,
        |   "student_id": "studentId1",
        |   "guardian_active_until": null,
        |   "guardian_created_time": "1970-07-14 02:41:00.0"
        |}
        |]
        |""".stripMargin

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
        |   "student_id": "studentId2",
        |   "student_status": 1,
        |   "student_created_time": "1970-07-13 02:41:00.0",
        |   "student_active_until": null
        |}
        |]
        """.stripMargin

    val registeredValue =
      """
        |[
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"GuardianRegistered",
        |   "createdOn":12346,
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-15 01:41:00.0",
        |   "guardianId":"guardian-id-3",
        |   "tenantId": "tenantId1",
        |   "invitationStatus": 2,
        |   "status": 1,
        |   "studentId": "studentId3"
        |}
        |]
        """.stripMargin

    val associationValue =
      """
        |[
        |{
        |   "guardianId":"guardian-id-3",
        |   "studentId":"studentId4",
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

    val transformer = new GuardianAssociationDeletedTransform(sprk, service)

    val deletedInput = spark.read.json(Seq(deletedValue).toDS())
    when(service.readOptional(ParquetGuardianDeletedSource, sprk)).thenReturn(Some(deletedInput))

    val relRedshiftInput = spark.read.json(Seq(relRedshiftValue).toDS())
    when(service.readFromRedshift[RelGuardian](RedshiftGuardianRelSink)).thenReturn(relRedshiftInput)

    val dimRedshiftInput = spark.read.json(Seq(dimRedshiftValue).toDS())
    when(service.readFromRedshift[DimGuardian](RedshiftGuardianDimSource)).thenReturn(dimRedshiftInput)

    val studentRedshiftInput = spark.read.json(Seq(studentRedshiftValue).toDS())
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(studentRedshiftInput)

    val registeredInput = spark.read.json(Seq(registeredValue).toDS())
    when(service.readOptional(GuardianRegistrationCommonTransformSink, sprk)).thenReturn(Some(registeredInput))

    val associationInput = spark.read.json(Seq(associationValue).toDS())
    when(service.readOptional(GuardianAssociationCommonTransformSink, sprk)).thenReturn(Some(associationInput))

    when(service.getStartIdUpdateStatus("dim_guardian")).thenReturn(1)

    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 4)

    val guardian1 = df.filter($"guardian_id" === "guardian-id-1")
    assert[Int](guardian1, "guardian_invitation_status", 2)
    assert[Int](guardian1, "guardian_status", 4)
    assert[String](guardian1, "student_id", "studentId1")
    assert[String](guardian1, "guardian_created_time", "1970-07-14 02:40:00.0")
    assert[String](guardian1, "guardian_active_until", null)
    assert[Int](guardian1, "rel_guardian_dw_id", 1)

    val guardian2 = df.filter($"guardian_id" === "guardian-id-2")
    assert[Int](guardian2, "guardian_invitation_status", 2)
    assert[Int](guardian2, "guardian_status", 4)
    assert[String](guardian2, "student_id", "studentId2")
    assert[String](guardian2, "guardian_created_time", "1970-07-14 02:40:00.0")
    assert[String](guardian2, "guardian_active_until", null)
    assert[Int](guardian2, "rel_guardian_dw_id", 2)

    val guardian3 = df.filter($"guardian_id" === "guardian-id-3")
    assert(guardian3.count() == 2)

    val student4 = guardian3.filter($"student_id" === "studentId4")
    assert[Int](student4, "guardian_invitation_status", 2)
    assert[Int](student4, "guardian_status", 4)
    assert[String](student4, "guardian_created_time", "1970-07-14 02:40:00.0")
    assert[String](student4, "guardian_active_until", null)
    assert[Int](student4, "rel_guardian_dw_id", 3)

    val student3 = guardian3.filter($"student_id" === "studentId3")
    assert[Int](student3, "guardian_invitation_status", 2)
    assert[Int](student3, "guardian_status", 4)
    assert[String](student3, "guardian_created_time", "1970-07-14 02:40:00.0")
    assert[String](student3, "guardian_active_until", null)
    assert[Int](student3, "rel_guardian_dw_id", 4)
  }

  test("guardian association deleted empty redshift should transform") {
    val deletedValue =
      """
        |[
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"GuardianDeleted",
        |   "events": [
        |     {"createdOn":111,"uuid":"guardian-id-1"},
        |     {"createdOn":333,"uuid":"guardian-id-3"}
        |   ],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181810",
        |   "occurredOn": "1970-07-14 02:40:00.0"
        |}
        |]
        |""".stripMargin

    val registeredValue =
      """
        |[
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"GuardianRegistered",
        |   "createdOn":12346,
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-15 01:41:00.0",
        |   "guardianId":"guardian-id-1",
        |   "tenantId": "tenantId1",
        |   "invitationStatus": 2,
        |   "status": 1,
        |   "studentId": "studentId3"
        |}
        |]
        """.stripMargin

    val associationValue =
      """
        |[
        |{
        |   "guardianId":"guardian-id-3",
        |   "studentId":"studentId4",
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

    val transformer = new GuardianAssociationDeletedTransform(sprk, service)

    val deletedInput = spark.read.json(Seq(deletedValue).toDS())
    when(service.readOptional(ParquetGuardianDeletedSource, sprk)).thenReturn(Some(deletedInput))

    val relRedshiftInput = Seq.empty[RelGuardian].toDF()
    when(service.readFromRedshift[RelGuardian](RedshiftGuardianRelSink)).thenReturn(relRedshiftInput)

    val dimRedshiftInput = Seq.empty[DimGuardian].toDF()
    when(service.readFromRedshift[DimGuardian](RedshiftGuardianDimSource)).thenReturn(dimRedshiftInput)

    val studentRedshiftInput = Seq.empty[StudentDim].toDF()
    when(service.readFromRedshift[StudentDim](RedshiftDimStudentSink)).thenReturn(studentRedshiftInput)

    val registeredInput = spark.read.json(Seq(registeredValue).toDS())
    when(service.readOptional(GuardianRegistrationCommonTransformSink, sprk)).thenReturn(Some(registeredInput))

    val associationInput = spark.read.json(Seq(associationValue).toDS())
    when(service.readOptional(GuardianAssociationCommonTransformSink, sprk)).thenReturn(Some(associationInput))

    when(service.getStartIdUpdateStatus("dim_guardian")).thenReturn(1)

    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)

    val guardian1 = df.filter($"guardian_id" === "guardian-id-1")
    assert[Int](guardian1, "guardian_invitation_status", 2)
    assert[Int](guardian1, "guardian_status", 4)
    assert[String](guardian1, "student_id", "studentId3")
    assert[String](guardian1, "guardian_created_time", "1970-07-14 02:40:00.0")
    assert[String](guardian1, "guardian_active_until", null)

    val validDwIds: Set[Long] = Set(1, 2)
    val result1DwId = guardian1.select("rel_guardian_dw_id").as[Long].head()
    assert(validDwIds.contains(result1DwId))

    val guardian3 = df.filter($"guardian_id" === "guardian-id-3")
    assert[Int](guardian3, "guardian_invitation_status", 2)
    assert[Int](guardian3, "guardian_status", 4)
    assert[String](guardian3, "student_id", "studentId4")
    assert[String](guardian3, "guardian_created_time", "1970-07-14 02:40:00.0")
    assert[String](guardian3, "guardian_active_until", null)

    val result3DwId = guardian3.select("rel_guardian_dw_id").as[Long].head()
    assert(validDwIds.contains(result3DwId))
  }
}
