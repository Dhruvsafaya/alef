package com.alefeducation.dimensions.guardian.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.Helpers.ParquetGuardianSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class GuardianRegistrationCommonTransformSpec extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("guardian registration common should transform") {
    val registeredValue =
      """
        |[
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"GuardianRegistered",
        |   "createdOn":12346,
        |   "uuid":"guardian-id-2",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-15 01:41:00.0"
        |},
        |{
        |   "tenantId":"tenant-id",
        |   "eventType":"RandomEvent",
        |   "createdOn": 123467,
        |   "uuid":"guardian-id-3",
        |   "loadtime":"wMC3yz1AAAAggyUA",
        |   "eventDateDw":"20181008",
        |   "occurredOn": "1970-07-15 01:41:00.0"
        |}
        |]
        """.stripMargin

    val expectedColumns = Set(
      "createdOn",
      "eventDateDw",
      "eventType",
      "loadtime",
      "occurredOn",
      "tenantId",
      "guardianId",
      "invitationStatus",
      "status",
      "studentId"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GuardianRegistrationCommonTransform(sprk, service)

    val registeredInput = spark.read.json(Seq(registeredValue).toDS())
    when(service.readOptional(ParquetGuardianSource, sprk)).thenReturn(Some(registeredInput))

    val sinks = transformer.transform()

    val df = sinks.get.output

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)

    val registered = df.filter($"guardianId" === "guardian-id-2")
    assert[Int](registered, "invitationStatus", 2)
    assert[Int](registered, "status", 1)
    assert[String](registered, "studentId", null)

    val wrongEvent = df.filter($"guardianId" === "guardian-id-3")
    assert[Int](wrongEvent, "invitationStatus", -1)
    assert[Int](wrongEvent, "status", 1)
    assert[String](wrongEvent, "studentId", null)
  }
}
