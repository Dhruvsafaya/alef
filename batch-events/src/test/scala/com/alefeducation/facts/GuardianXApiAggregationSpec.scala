package com.alefeducation.facts

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.xapi.GuardianXAPITransformer
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers._

class GuardianXApiAggregationSpec extends SparkSuite {

  trait Setup {
    implicit val transformer = new GuardianXAPITransformer(GuardianXapiV2Service, spark)
  }

  test("guardian app accessed and student selected events") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetGuardianXApiV2Source,
          value =
            """
              |[{
              |  "eventType":"guardian.app.activity.accessed",
              |  "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              |  "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/guardian\",\"name\":\"8565c1e3-c079-45c3-9eec-31051aefd523\"}},\"verb\":{\"id\":\"https://w3id.org/xapi/dod-isd/verbs/accessed\",\"display\":{\"en-US\":\"accessed\"}},\"object\":{\"id\":\"http://alefeducation.com/xapi/activities/app/guardian\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{},\"extensions\":{\"http://alefeducation.com\":{\"timestampLocal\":\"2019-06-18T06:43:41.279Z..................................................................................................................\",\"device\":\"ios\",\"userAgent\":\"Expo/2.11.1.106135 CFNetwork/975.0.3 Darwin/17.7.0\",\"student\":{\"id\":\"184f2cdd-f21d-4273-aab6-1371b52fb2e7\"},\"outsideOfSchool\":true,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"8fef857b-6517-4ad4-960a-a554de803ca6\"}}}}},\"result\":{},\"timestamp\":\"2019-06-18T06:44:00.815Z\"}",
              |  "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              |  "eventType":"guardian.app.student.selected",
              |  "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              |  "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/guardian\",\"name\":\"8565c1e3-c079-45c3-9eec-31051aefd523\"}},\"verb\":{\"id\":\"https://w3id.org/xapi/dod-isd/verbs/selected\",\"display\":{\"en-US\":\"selected\"}},\"object\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/student\",\"name\":\"583770ec-b583-45ab-97aa-6d0a09cc941b\"}},\"context\":{\"contextActivities\":{},\"extensions\":{\"http://alefeducation.com\":{\"timestampLocal\":\"2019-06-18T06:43:41.279Z\",\"device\":\"ios\",\"student\":{\"id\":\"583770ec-b583-45ab-97aa-6d0a09cc941b\"},\"userAgent\":\"Expo/2.11.1.106135 CFNetwork/975.0.3 Darwin/17.7.0\",\"outsideOfSchool\":true,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"8fef857b-6517-4ad4-960a-a554de803ca6\"}}}}},\"result\":{},\"timestamp\":\"2019-06-18T06:44:06.359Z\"}",
              |  "loadtime":"QFDTSYoTAADlgyUA"
              |},
              |{
              |  "eventType":"guardian.app.student.selected",
              |  "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              |  "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/guardian\",\"name\":\"8565c1e3-c079-45c3-9eec-31051aefd524\"}},\"verb\":{\"id\":\"https://w3id.org/xapi/dod-isd/verbs/selected\",\"display\":{\"en-US\":\"selected\"}},\"object\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/student\",\"name\":\"583770ec-b583-45ab-97aa-6d0a09cc941b\"}},\"context\":{\"contextActivities\":{},\"extensions\":{\"http://alefeducation.com\":{\"timestampLocal\":\"2019-06-18T06:43:41.279Z\",\"device\":\"ios\",\"userAgent\":\"Expo/2.11.1.106135 CFNetwork/975.0.3 Darwin/17.7.0\",\"outsideOfSchool\":true,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\"}}}},\"result\":{},\"timestamp\":\"2019-06-18T06:44:06.340Z\"}",
              |  "loadtime":"QFDTSYoTAADlgyUA"
              |}]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetSink = sinks.find(_.name == ParquetGuardianXApiV2Source)
          assert(parquetSink.size == 1)
          val parquetDf = parquetSink.head.output
          assert(parquetDf.count == 3)
          assert(parquetSink.head.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate,eventType"))
          val record1 = parquetDf.filter("eventType = 'guardian.app.activity.accessed'")
          assert(record1.count == 1)
          val record2 = parquetDf.filter("eventType = 'guardian.app.student.selected'")
          assert(record2.count == 2)

          val redshiftSink = sinks.find(_.name == RedshiftGuardianAppActivitiesSink)
          assert(redshiftSink.size == 1)

          val redshiftDf = redshiftSink.head.output.cache()
          assert(redshiftDf.count == 3)

          val redshiftGuardianAppAccessedDf =
            redshiftDf.filter("fgaa_event_type = 'guardian.app.activity.accessed'")
          assert[String](redshiftGuardianAppAccessedDf, "fgaa_event_type", "guardian.app.activity.accessed")
          assert[String](redshiftGuardianAppAccessedDf, "fgaa_object_type", "Activity")
          assert[String](redshiftGuardianAppAccessedDf, "fgaa_object_id", "http://alefeducation.com/xapi/activities/app/guardian")
          assert[String](redshiftGuardianAppAccessedDf, "fgaa_actor_account_homepage", "http://alefeducation.com/users/guardian")
          assert[String](redshiftGuardianAppAccessedDf, "fgaa_object_account_homepage", null)
          assert[String](redshiftGuardianAppAccessedDf, "tenant_uuid", "93e4949d-7eff-4707-9201-dac917a5e013")
          assert[String](redshiftGuardianAppAccessedDf, "fgaa_device", "ios")
          assert[String](redshiftGuardianAppAccessedDf, "school_uuid", "8fef857b-6517-4ad4-960a-a554de803ca6")
          assert[String](redshiftGuardianAppAccessedDf, "student_uuid", "184f2cdd-f21d-4273-aab6-1371b52fb2e7")
          assert[String](redshiftGuardianAppAccessedDf, "guardian_uuid", "8565c1e3-c079-45c3-9eec-31051aefd523")
          assert[String](redshiftGuardianAppAccessedDf, "fgaa_actor_object_type", "Agent")
          assert[String](redshiftGuardianAppAccessedDf, "fgaa_verb_id", "https://w3id.org/xapi/dod-isd/verbs/accessed")
          assert(redshiftGuardianAppAccessedDf.first.getAs[String]("fgaa_timestamp_local").length === EndIndexTimestampLocal)

          val redshiftStudentSelectedDf =
            redshiftDf.filter("fgaa_event_type = 'guardian.app.student.selected'")
          assert[String](redshiftStudentSelectedDf, "fgaa_event_type", "guardian.app.student.selected")
          assert[String](redshiftStudentSelectedDf, "fgaa_object_type", "Agent")
          assert[String](redshiftStudentSelectedDf, "tenant_uuid", "93e4949d-7eff-4707-9201-dac917a5e013")
          assert[String](redshiftStudentSelectedDf, "fgaa_device", "ios")
          assert[String](redshiftStudentSelectedDf, "fgaa_object_id", null)
          assert[String](redshiftStudentSelectedDf, "school_uuid", "8fef857b-6517-4ad4-960a-a554de803ca6")
          assert[String](redshiftStudentSelectedDf, "student_uuid", "583770ec-b583-45ab-97aa-6d0a09cc941b")
          assert[String](redshiftStudentSelectedDf, "guardian_uuid", "8565c1e3-c079-45c3-9eec-31051aefd523")
          assert[String](redshiftStudentSelectedDf, "fgaa_actor_object_type", "Agent")
          assert[String](redshiftStudentSelectedDf, "fgaa_verb_id", "https://w3id.org/xapi/dod-isd/verbs/selected")

          val redshiftStudentSelectedWithDefaultsDf =
            redshiftDf.filter("guardian_uuid = '8565c1e3-c079-45c3-9eec-31051aefd524'")
          assert[String](redshiftStudentSelectedWithDefaultsDf, "fgaa_object_id", null)
          assert[String](redshiftStudentSelectedWithDefaultsDf, "school_uuid", DEFAULT_ID)
          assert[String](redshiftStudentSelectedWithDefaultsDf, "student_uuid", DEFAULT_ID)

          val deltaSink = sinks.find(_.name == DeltaGuardianAppActivitiesSink)
          val deltaDf = deltaSink.head.output.cache()
          val expCols = List(
            "fgaa_tenant_id",
            "fgaa_guardian_id",
            "fgaa_actor_account_homepage",
            "fgaa_object_account_homepage",
            "fgaa_actor_object_type",
            "fgaa_object_type",
            "fgaa_verb_id",
            "fgaa_verb_display",
            "fgaa_object_id",
            "fgaa_device",
            "fgaa_school_id",
            "fgaa_student_id",
            "fgaa_event_type",
            "fgaa_timestamp_local",
            "fgaa_date_dw_id",
            "fgaa_created_time",
            "fgaa_dw_created_time",
            "eventdate"
          )
          assert(deltaDf.columns.diff(expCols).length == 0)
          assert(deltaSink.size == 1)
          assert(deltaDf.count == 3)

          val deltaGuardianAppAccessedDf =
            deltaDf.filter("fgaa_event_type = 'guardian.app.activity.accessed'").cache()
          assert[String](deltaGuardianAppAccessedDf, "fgaa_event_type", "guardian.app.activity.accessed")
          assert[String](deltaGuardianAppAccessedDf, "fgaa_object_type", "Activity")
          assert[String](deltaGuardianAppAccessedDf, "fgaa_object_id", "http://alefeducation.com/xapi/activities/app/guardian")
          assert[String](deltaGuardianAppAccessedDf, "fgaa_actor_account_homepage", "http://alefeducation.com/users/guardian")
          assert[String](deltaGuardianAppAccessedDf, "fgaa_object_account_homepage", null)
          assert[String](deltaGuardianAppAccessedDf, "fgaa_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
          assert[String](deltaGuardianAppAccessedDf, "fgaa_device", "ios")
          assert[String](deltaGuardianAppAccessedDf, "fgaa_school_id", "8fef857b-6517-4ad4-960a-a554de803ca6")
          assert[String](deltaGuardianAppAccessedDf, "fgaa_student_id", "184f2cdd-f21d-4273-aab6-1371b52fb2e7")
          assert[String](deltaGuardianAppAccessedDf, "fgaa_guardian_id", "8565c1e3-c079-45c3-9eec-31051aefd523")
          assert[String](deltaGuardianAppAccessedDf, "fgaa_actor_object_type", "Agent")
          assert[String](deltaGuardianAppAccessedDf, "fgaa_verb_id", "https://w3id.org/xapi/dod-isd/verbs/accessed")
          assert(deltaGuardianAppAccessedDf.first.getAs[String]("fgaa_timestamp_local").length === EndIndexTimestampLocal)

          val deltaStudentSelectedDf =
            deltaDf.filter("fgaa_event_type = 'guardian.app.student.selected'").cache()
          assert[String](deltaStudentSelectedDf, "fgaa_event_type", "guardian.app.student.selected")
          assert[String](deltaStudentSelectedDf, "fgaa_object_type", "Agent")
          assert[String](deltaStudentSelectedDf, "fgaa_tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
          assert[String](deltaStudentSelectedDf, "fgaa_device", "ios")
          assert[String](deltaStudentSelectedDf, "fgaa_object_id", null)
          assert[String](deltaStudentSelectedDf, "fgaa_school_id", "8fef857b-6517-4ad4-960a-a554de803ca6")
          assert[String](deltaStudentSelectedDf, "fgaa_student_id", "583770ec-b583-45ab-97aa-6d0a09cc941b")
          assert[String](deltaStudentSelectedDf, "fgaa_guardian_id", "8565c1e3-c079-45c3-9eec-31051aefd523")
          assert[String](deltaStudentSelectedDf, "fgaa_actor_object_type", "Agent")
          assert[String](deltaStudentSelectedDf, "fgaa_verb_id", "https://w3id.org/xapi/dod-isd/verbs/selected")

          val deltaStudentSelectedWithDefaultsDf =
            deltaDf.filter("fgaa_guardian_id = '8565c1e3-c079-45c3-9eec-31051aefd524'").cache()
          assert[String](deltaStudentSelectedWithDefaultsDf, "fgaa_object_id", null)
          assert[String](deltaStudentSelectedWithDefaultsDf, "fgaa_school_id", DEFAULT_ID)
          assert[String](deltaStudentSelectedWithDefaultsDf, "fgaa_student_id", DEFAULT_ID)

        }
      )
    }
  }

}
