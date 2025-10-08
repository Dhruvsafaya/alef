package com.alefeducation.dimensions.guardian.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Helpers.ParquetGuardianAssociationsSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class GuardianAssociationCommonTransformSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("guardian association common should transform") {
    val associationValue =
      """
        |[
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"GuardianAssociationsUpdated",
        |   "createdOn":12345,
        |   "studentId":"student-id",
        |   "guardians":[{"id":"guardian-id-2"}],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-15 02:40:00.0"
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"GuardianAssociationsUpdated",
        |   "createdOn":12346,
        |   "studentId":"student-id",
        |   "guardians":[{"id":"guardian-id-2"}, {"id":"guardian-id-3"}, {"id":"guardian-id-1"}],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-15 03:55:00.0"
        |}
        |]
        """.stripMargin

    val expectedColumns = Set(
      "guardianId",
      "studentId",
      "occurredOn",
      "status",
      "invitationStatus"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GuardianAssociationCommonTransform(sprk, service)

    val associationInput = spark.read.json(Seq(associationValue).toDS())
    when(service.readOptional(ParquetGuardianAssociationsSource, sprk)).thenReturn(Some(associationInput))

    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 3)

    val df3 = df.filter($"guardianId" === "guardian-id-3")
    assert[String](df3, "studentId", "student-id")
    assert[String](df3, "occurredOn", "1970-07-15 03:55:00.0")
    assert[Int](df3, "status", 1)
    assert[Int](df3, "invitationStatus", 2)
  }

  test("guardian association common with empty guardians should return None") {
    val associationValue =
      """
        |[
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"GuardianAssociationsUpdated",
        |   "createdOn":12346,
        |   "studentId":"student-id",
        |   "guardians":[],
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-15 03:55:00.0"
        |}
        |]
        """.stripMargin

    val sprk = spark
    import sprk.implicits._

    val transformer = new GuardianAssociationCommonTransform(sprk, service)

    val associationInput = spark.read.json(Seq(associationValue).toDS())
    when(service.readOptional(ParquetGuardianAssociationsSource, sprk)).thenReturn(Some(associationInput))

    val sinks = transformer.transform()

    assert(sinks.isEmpty)
  }

}
