package com.alefeducation.facts.announcement
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.announcement.AnnouncementUtils._
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class AnnouncementUtilsSpec extends SparkSuite {

  val sprk: SparkSession = spark

  val message: String = """
                |[
                |{
                |   "eventType": "TeacherAnnouncementSentEvent",
                |   "announcementId": "65cf3dd8-b819-446d-b0bf-b7216b7bbba9",
                |   "teacherId": "c3bed03b-19ff-4718-b6bf-17a359a349cd",
                |   "schoolId": "school_uuid1",
                |   "classIds": null,
                |	  "studentIds": [
                |       "74a9f731-4545-4fcf-8f9b-2aea397f69f1",
                |       "758a61c9-f7a1-416a-b003-ab15e381a3d7"
                |   ],
                |   "hasAttachment": false,
                |   "occurredOn": "2022-11-17 09:29:24.430",
                |   "eventDateDw": 20221117,
                |   "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
                |}]
        """.stripMargin

  test("addRoleName should add roleName column to dataframe") {
    val expVal = "someRole"
    val df = addRoleName(expVal)(createDf())

    assert[String](df, "roleName", expVal)
  }

  test("addStatus should add fa_status column to dataframe") {
    val expStatus = 99
    val df = addStatus(expStatus)(createDf())

    assert[Int](df, "fa_status", expStatus)
  }

  test("createRecipientTypeDesc should create recipientTypeDesc") {
    val df = createRecipientTypeDesc(StudentIdsColName)(createDf())

    assert[String](df, "recipientTypeDesc", StudentTypeDesc)
  }

  test("createRecipientTypeDesc should create recipientTypeDesc with value UNKNOWN when an unknown column passed") {
    val df = createRecipientTypeDesc("UnknownColName")(createDf())

    assert[String](df, "recipientTypeDesc", UnknownTypeDesc)
  }

  test("createRecipientTypeId should create recipientTypeId") {
    val df = createRecipientTypeId(StudentIdsColName)(createDf())

    assert[Int](df, "recipientType", StudentTypeId)
  }

  test("createRecipientType should create recipientType with value -1 when an unknown column passed") {
    val df = createRecipientTypeId("UnknownColName")(createDf())

    assert[Int](df, "recipientType", UnknownTypeId)
  }

  test("explodeRecipients should create exploded recipientId column") {
    val df = explodeRecipients(StudentIdsColName)(createDf())

    assertSet[String](df, "recipientId", Set(
      "74a9f731-4545-4fcf-8f9b-2aea397f69f1",
      "758a61c9-f7a1-416a-b003-ab15e381a3d7"
    ))
  }

  test("transformColumnIntoArray should transform single value column into array") {
    val df = transformColumnIntoArray(SchoolIdColName)(createDf())

    assert(df.schema(SchoolIdColName).dataType == ArrayType(StringType, containsNull = true))
  }

  test("addType should create fa_type column with students value") {
    val message =
      """
        |[
        |{
        |   "announcementType": "STUDENTS"
        |}
        |]
        |""".stripMargin

    val inputDf = createDf(message)

    val df = addType(inputDf)

    assert[Int](df, "fa_type", StudentsAnnTypeVal)
  }

  test("addType should create fa_type column with guardians value") {
    val message =
      """
        |[
        |{
        |   "announcementType": "GUARDIANS"
        |}
        |]
        |""".stripMargin

    val inputDf = createDf(message)

    val df = addType(inputDf)

    assert[Int](df, "fa_type", GuardiansAnnTypeVal)
  }

  test("addType should create fa_type column with guardians and students value") {
    val message =
      """
        |[
        |{
        |   "announcementType": "GUARDIANS_AND_STUDENTS"
        |}
        |]
        |""".stripMargin

    val inputDf = createDf(message)

    val df = addType(inputDf)

    assert[Int](df, "fa_type", GuardiansAndStudentsAnnTypeVal)
  }

  test("addType should create fa_type column with null value") {
    val message =
      """
        |[
        |{
        |   "announcementType": "ANY UNKNOWN VALUES"
        |}
        |]
        |""".stripMargin

    val inputDf = createDf(message)

    val df = addType(inputDf)

    assert[String](df, "fa_type", null)
  }

  private def createDf(msg: String = message): DataFrame = {
    val sprk = spark
    import sprk.implicits._
    spark.read.json(Seq(msg).toDS())
  }
}
