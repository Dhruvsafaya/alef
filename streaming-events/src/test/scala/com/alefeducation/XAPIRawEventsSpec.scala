package com.alefeducation

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, explode}
import org.scalatest.matchers.should.Matchers

class XAPIRawEventsSpec extends SparkSuite with Matchers {

  trait Setup {
    val session = spark

    import session.implicits._

    implicit val transformer = new NewXAPIRawEventsTransformer("new-xapi-events", session)

    def explodeArr(df: DataFrame, name: String): DataFrame = {
      df.select(explode(col(name))).select($"col.*")
    }
  }

  val expectedSchoolMutatedColumns: Set[String] = Set(
    "eventType",
    "tenantId",
    "createdOn",
    "uuid",
    "addressId",
    "addressLine",
    "addressPostBox",
    "addressCity",
    "addressCountry",
    "organisation",
    "latitude",
    "longitude",
    "timeZone",
    "firstTeachingDay",
    "name",
    "composition",
    "eventDateDw",
    "occurredOn",
    "loadtime",
    "alias",
    "sourceId"
  )

  val expectedSectionScheduleModifiedColumns: Set[String] = Set(
    "eventType",
    "eventDateDw",
    "loadtime",
    "occurredOn",
    "slots",
    "tenantId",
    "sectionId"
  )

  test("filterout invalid xapi eventTypes") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = "new-xapi-source",
          value = """
              |[
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "À®À®ÀÀ®À®ÀÀ®À®ÀÀ®À®ÀÀ®À®ÀÀ®À®ÀÀ®À®ÀÀ®À®ÀÀ®À®ÀÀ®À®ÀwindowsÀwin.ini",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "createdOn":1575965594444,
              |     "uuid": "6c56d3b2-89b9-4c01-8f6f-614fa907151b"
              |   }
              | },
              | "timestamp": "2019-04-20 16:24:37.501"
              |},
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "...%2Fguardian.app.activity.accessed",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "createdOn":1575965594444,
              |     "uuid": "6c56d3b2-89b9-4c01-8f6f-614fa907151b"
              |   }
              | },
              | "timestamp": "2019-04-20 16:24:37.501"
              |},
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "...%5C.%5C...%5C.%5C...%5C.%5C...%5C.%5C...%5C.%5C...%5C.%5C...%5C.%5C...%5C.%5C...%5C.%5C...%5C.%5Cwindows%5Cwin.ini/",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "createdOn":1575965594444,
              |     "uuid": "6c56d3b2-89b9-4c01-8f6f-614fa907151b"
              |   }
              | },
              | "timestamp": "2019-04-20 16:24:37.501"
              |},
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "..%2F..%2F..%2F..%2F..%2F..%2F..%2F..%2F..%2F..%2F..%2F..%2F..%2F..%2F..%2F..%2Fetc%2Fpasswd/",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "createdOn":1575965594444,
              |     "uuid": "6c56d3b2-89b9-4c01-8f6f-614fa907151b"
              |   }
              | },
              | "timestamp": "2019-04-20 16:24:37.501"
              |},
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "guardian.app.activity.accessed",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "createdOn":1575965594444,
              |     "uuid": "6c56d3b2-89b9-4c01-8f6f-614fa907151b"
              |   }
              | },
              | "timestamp": "2019-04-20 16:24:37.501"
              |},
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "teacher.lessonReportPage.performanceItem.closed",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "createdOn":1575965594444,
              |     "uuid": "6c56d3b2-89b9-4c01-8f6f-614fa907151b"
              |   }
              | },
              | "timestamp": "2019-04-20 16:24:37.501"
              |},
              |{
              | "key": "key1",
              | "value":{
              |  "headers": {
              |    "eventType": "tdc.plat_form.logout",
              |    "tenantId": "93e4949d-7eff-4707-9201-dac917a5e013"
              |  },
              |  "body": {
              |     "createdOn":1575965594444,
              |     "uuid": "6c56d3b2-89b9-4c01-8f6f-614fa907151b"
              |   }
              | },
              | "timestamp": "2019-04-20 16:24:37.501"
              |}
              |]
            """.stripMargin
        )
      )

      withSparkFixtures(
        fixtures, { sinks =>
          val df: DataFrame = sinks
            .find(_.name == "new-xapi-sink")
            .map(_.input)
            .getOrElse(throw new RuntimeException("new-xapi-sink is not found"))

          val fst = df.first()
          fst.getAs[String]("eventType") shouldBe "guardian.app.activity.accessed"
        }
      )
    }
  }
}
