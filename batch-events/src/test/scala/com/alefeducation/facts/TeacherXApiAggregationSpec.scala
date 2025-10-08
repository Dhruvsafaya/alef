package com.alefeducation.facts

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.xapi.TeacherXAPITransformer
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers._

class TeacherXApiAggregationSpec extends SparkSuite {

  trait Setup {
    implicit val transformer = new TeacherXAPITransformer(TeacherXapiV2Service, spark)
  }

  test("test login and logout events and limit timestampLocal to max 80 characters") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetTeacherXApiV2Source,
          value =
            """
              |[{
              |  "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              |  "eventType": "teacher.platform.login",
              |  "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/login/\",\"display\":{\"en-US\":\"Log In\"}},\"object\":{\"id\":\"http://alefeducation.com/login\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T15:30:59.572Z.........................................................................................................................................................................\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\",\"outsideOfSchool\": false,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:30:59.650Z\"}",
              |  "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.platform.logout",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/logout/\",\"display\":{\"en-US\":\"Log Out\"}},\"object\":{\"id\":\"http://alefeducation.com/logout\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T05:45:30.992Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36\",\"outsideOfSchool\": false,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:31:02.650Z\"}",
              | "loadtime":"wH+Z1NoSAADlgyUA"
              |}]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetSink = sinks.find(_.name == ParquetTeacherXApiV2Source)
          assert(parquetSink.size == 1)
          val parquetDf = parquetSink.head.output
          assert(parquetDf.count == 2)
          assert(parquetSink.head.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate,eventType"))
          val record1 = parquetDf.filter("eventType = 'teacher.platform.login'")
          assert(record1.count == 1)
          val record2 = parquetDf.filter("eventType = 'teacher.platform.logout'")
          assert(record2.count == 1)

          val redshiftSink = sinks.find(_.name == RedshiftTeacherActivitiesSink)
          assert(redshiftSink.size == 1)
          val redshiftDf = redshiftSink.head.output
          assert(redshiftDf.count == 2)
          val redshiftRow = redshiftDf.first()
          assertRow[Double](redshiftRow, "fta_time_spent", 3)
          assertRow[String](redshiftRow, "fta_event_type", "teacher.platform.login")
          assertRow[String](redshiftRow, "fta_prev_event_type", null)
          assertRow[String](redshiftRow, "fta_next_event_type", "teacher.platform.logout")
          assertRow[String](redshiftRow, "tenant_uuid", "93e4949d-7eff-4707-9201-dac917a5e013")
          assertRow[String](redshiftRow, "teacher_uuid", "afd49ed3-f105-4a56-a908-3b1301c3edbb")
          assertRow[String](redshiftRow, "school_uuid", "1cdba148-3e14-4abd-8fee-6829537a25c6")
          assertRow[String](redshiftRow, "grade_uuid", DEFAULT_ID)
          assertRow[String](redshiftRow, "section_uuid", DEFAULT_ID)
          assertRow[String](redshiftRow, "fta_actor_object_type", "Agent")
          assertRow[String](redshiftRow, "fta_actor_account_homepage", "http://alefeducation.com")
          assertRow[String](redshiftRow, "fta_verb_id", "https://brindlewaye.com/xAPITerms/verbs/login/")
          assertRow[String](redshiftRow, "fta_object_id", "http://alefeducation.com/login")
          assert(redshiftRow.getAs[String]("fta_timestamp_local").length === EndIndexTimestampLocal)
          assertRow[Boolean](redshiftRow, "fta_outside_of_school", false)

          val invalidSink = sinks.find(_.name == ParquetInvalidTeacherXApiV2Sink)
          assert(invalidSink.isEmpty)
        }
      )
    }
  }

  test("login, analytics pages and logout events") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetTeacherXApiV2Source,
          value =
            """
              |[{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.platform.login",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/login/\",\"display\":{\"en-US\":\"Log In\"}},\"object\":{\"id\":\"http://alefeducation.com/login\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T15:30:59.572Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\",\"outsideOfSchool\": true,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:30:59.650Z\"}",
              | "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.class.selected",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"http://id.tincanapi.com/verb/selected\",\"display\":{\"en-US\":\"class selected\"}},\"object\":{\"id\":\"http://alefeducation.com/class.filter\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"https://w3id.org/xapi/seriousgames/activity-types/menu\",\"name\":{\"en-US\":\"6A\"}}},\"context\":{\"contextActivities\":{\"parent\":[{\"id\":\"http://alefeducation.com/#/analytics/tab/classPerformance\"},{\"id\":\"http://alefeducation.com/learning-path/88b387ac-5f56-41d2-b7bd-ca1685bb3a8e\"}],\"grouping\":[{\"id\":\"http://alefeducation.com/curriculum/392027/grade/596550/subject/571671\"}],\"category\":[{\"id\":\"http://alefeducation.com/class\"}]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T14:43:10.785Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\",\"outsideOfSchool\": true,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{\"id\":\"462dfaa3-0088-4016-9974-5bf55df25f23\"},\"subject\":{\"id\":\"d79aeb6b-537b-4051-9a8d-45d4957b6c34\",\"name\":\"Math\"},\"section\":{\"id\":\"5a94144a-ae63-45b5-bcbc-8a93a3874d93\"}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:35:59.650Z\"}",
              | "loadtime":"QK447zEwAADlgyUA"
              |},
              |{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.classPerformance.accessed",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://w3id.org/xapi/seriousgames/verbs/accessed\",\"display\":{\"en-US\":\"Class Performance Accessed\"}},\"object\":{\"id\":\"http://alefeducation.com/class-performance-page\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://alefeducation.com/xapi/activities/analytics\"}},\"context\":{\"contextActivities\":{\"parent\":[{\"id\":\"http://alefeducation.com/class-performance-table\"},{\"id\":\"http://alefeducation.com/learning-path/c672c131-e655-40ef-b413-2a42be895a63\"}],\"grouping\":[{\"id\":\"http://alefeducation.com/curriculum/392027/grade/596550/subject/352071\"}],\"category\":[{\"id\":\"http://alefeducation.com/classPerformance\"}]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T05:58:04.577Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36\",\"outsideOfSchool\": true,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{\"id\":\"6e796516-72d5-4b71-9923-cac39b8d6109\"},\"subject\":{\"id\":\"f6523b83-c0e7-47f1-a276-15b384389bc0\",\"name\":\"Arabic\"},\"section\":{\"id\":\"9095e929-b8c3-4da9-829f-f4353edc1cfa\"}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:36:15.650Z\"}",
              | "loadtime":"QFDTSYoTAADlgyUA"
              |},
              |{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.grade.selected",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"http://id.tincanapi.com/verb/selected\",\"display\":{\"en-US\":\"grade selected\"}},\"object\":{\"id\":\"http://alefeducation.com/grade.filter\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"https://w3id.org/xapi/seriousgames/activity-types/menu\",\"name\":{\"en-US\":7}}},\"context\":{\"contextActivities\":{\"parent\":[{\"id\":\"http://alefeducation.com/#/analytics/tab/classPerformance\"},{\"id\":\"http://alefeducation.com/learning-path/88b387ac-5f56-41d2-b7bd-ca1685bb3a8e\"}],\"grouping\":[{\"id\":\"http://alefeducation.com/curriculum/392027/grade/596550/subject/571671\"}],\"category\":[{\"id\":\"http://alefeducation.com/grade\"}]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T14:00:49.825Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\",\"outsideOfSchool\": true,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{\"id\":\"462dfaa3-0088-4016-9974-5bf55df25f23\"},\"subject\":{\"id\":\"d79aeb6b-537b-4051-9a8d-45d4957b6c34\",\"name\":\"Math\"},\"section\":{\"id\":\"5a94144a-ae63-45b5-bcbc-8a93a3874d93\"}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:36:17.650Z\"}",
              | "loadtime":"AKSDSuItAADlgyUA"
              |},
              |{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.platform.logout",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/logout/\",\"display\":{\"en-US\":\"Log Out\"}},\"object\":{\"id\":\"http://alefeducation.com/logout\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T05:45:30.992Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36\",\"outsideOfSchool\": true,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"3b4775c8-2727-4e29-abfc-3d2cdfe92311\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:37:15.650Z\"}",
              | "loadtime":"wH+Z1NoSAADlgyUA"
              |}
              |]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val redshiftSink = sinks.find(_.name == RedshiftTeacherActivitiesSink)
          val redshiftDf = redshiftSink.head.output
          assert(redshiftDf.count == 5)
          val record1 = redshiftDf.filter("fta_start_time = '2019-04-23 15:30:59.65'").first()
          assertRow[Double](record1, "fta_time_spent", 300)
          assertRow[String](record1, "teacher_uuid", "afd49ed3-f105-4a56-a908-3b1301c3edbb")
          assertRow[String](record1, "school_uuid", "1cdba148-3e14-4abd-8fee-6829537a25c6")
          assertRow[String](record1, "grade_uuid", DEFAULT_ID)
          assertRow[String](record1, "section_uuid", DEFAULT_ID)
          assertRow[String](record1, "subject_uuid", DEFAULT_ID)
          assertRow[String](record1, "fta_event_type", "teacher.platform.login")
          assertRow[String](record1, "fta_prev_event_type", null)
          assertRow[String](record1, "fta_next_event_type", "teacher.class.selected")
          assertTimestampRow(record1, "fta_end_time", "2019-04-23 15:35:59.65")
          assertRow[Int](record1, "fta_date_dw_id", 20190423)
          assertRow[Boolean](record1, "fta_outside_of_school", true)

          val record2 = redshiftDf.filter("fta_start_time = '2019-04-23 15:35:59.65'").first()
          assertRow[Double](record2, "fta_time_spent", 16)
          assertRow[String](record2, "teacher_uuid", "afd49ed3-f105-4a56-a908-3b1301c3edbb")
          assertRow[String](record2, "school_uuid", "1cdba148-3e14-4abd-8fee-6829537a25c6")
          assertRow[String](record2, "grade_uuid", "462dfaa3-0088-4016-9974-5bf55df25f23")
          assertRow[String](record2, "section_uuid", "5a94144a-ae63-45b5-bcbc-8a93a3874d93")
          assertRow[String](record2, "subject_uuid", "d79aeb6b-537b-4051-9a8d-45d4957b6c34")
          assertRow[String](record2, "fta_event_type", "teacher.class.selected")
          assertRow[String](record2, "fta_prev_event_type", "teacher.platform.login")
          assertRow[String](record2, "fta_next_event_type", "teacher.classPerformance.accessed")
          assertTimestampRow(record2, "fta_end_time", "2019-04-23 15:36:15.65")
          assertRow[Int](record2, "fta_date_dw_id", 20190423)
          assertRow[Boolean](record2, "fta_outside_of_school", true)

          val record3 = redshiftDf.filter("fta_start_time = '2019-04-23 15:36:15.65'").first()
          assertRow[Double](record3, "fta_time_spent", 2)
          assertRow[String](record3, "teacher_uuid", "afd49ed3-f105-4a56-a908-3b1301c3edbb")
          assertRow[String](record3, "school_uuid", "1cdba148-3e14-4abd-8fee-6829537a25c6")
          assertRow[String](record3, "grade_uuid", "6e796516-72d5-4b71-9923-cac39b8d6109")
          assertRow[String](record3, "section_uuid", "9095e929-b8c3-4da9-829f-f4353edc1cfa")
          assertRow[String](record3, "subject_uuid", "f6523b83-c0e7-47f1-a276-15b384389bc0")
          assertRow[String](record3, "fta_event_type", "teacher.classPerformance.accessed")
          assertRow[String](record3, "fta_prev_event_type", "teacher.class.selected")
          assertRow[String](record3, "fta_next_event_type", "teacher.grade.selected")
          assertTimestampRow(record3, "fta_end_time", "2019-04-23 15:36:17.65")
          assertRow[Int](record3, "fta_date_dw_id", 20190423)
          assertRow[Boolean](record3, "fta_outside_of_school", true)

          val record4 = redshiftDf.filter("fta_start_time = '2019-04-23 15:36:17.65'").first()
          assertRow[Double](record4, "fta_time_spent", 58)
          assertRow[String](record4, "teacher_uuid", "afd49ed3-f105-4a56-a908-3b1301c3edbb")
          assertRow[String](record4, "school_uuid", "1cdba148-3e14-4abd-8fee-6829537a25c6")
          assertRow[String](record4, "grade_uuid", "462dfaa3-0088-4016-9974-5bf55df25f23")
          assertRow[String](record4, "section_uuid", "5a94144a-ae63-45b5-bcbc-8a93a3874d93")
          assertRow[String](record4, "subject_uuid", "d79aeb6b-537b-4051-9a8d-45d4957b6c34")
          assertRow[String](record4, "fta_event_type", "teacher.grade.selected")
          assertRow[String](record4, "fta_prev_event_type", "teacher.classPerformance.accessed")
          assertRow[String](record4, "fta_next_event_type", "teacher.platform.logout")
          assertTimestampRow(record4, "fta_end_time", "2019-04-23 15:37:15.65")
          assertRow[Int](record4, "fta_date_dw_id", 20190423)
          assertRow[Boolean](record4, "fta_outside_of_school", true)

          val invalidSink = sinks.find(_.name == ParquetInvalidTeacherXApiV2Sink)
          assert(invalidSink.isEmpty)
        }
      )
    }
  }

  test("login, analytics pages events with one event more than 30 minutes apart") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetTeacherXApiV2Source,
          value =
            """
              |[{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.platform.login",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/login/\",\"display\":{\"en-US\":\"Log In\"}},\"object\":{\"id\":\"http://alefeducation.com/login\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T15:30:59.572Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\",\"outsideOfSchool\": true,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:30:59.650Z\"}",
              | "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.class.selected",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"http://id.tincanapi.com/verb/selected\",\"display\":{\"en-US\":\"class selected\"}},\"object\":{\"id\":\"http://alefeducation.com/class.filter\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"https://w3id.org/xapi/seriousgames/activity-types/menu\",\"name\":{\"en-US\":\"6A\"}}},\"context\":{\"contextActivities\":{\"parent\":[{\"id\":\"http://alefeducation.com/#/analytics/tab/classPerformance\"},{\"id\":\"http://alefeducation.com/learning-path/88b387ac-5f56-41d2-b7bd-ca1685bb3a8e\"}],\"grouping\":[{\"id\":\"http://alefeducation.com/curriculum/392027/grade/596550/subject/571671\"}],\"category\":[{\"id\":\"http://alefeducation.com/class\"}]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T14:43:10.785Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\",\"outsideOfSchool\": true,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{\"id\":\"462dfaa3-0088-4016-9974-5bf55df25f23\"},\"subject\":{\"id\":\"d79aeb6b-537b-4051-9a8d-45d4957b6c34\",\"name\":\"Math\"},\"section\":{\"id\":\"5a94144a-ae63-45b5-bcbc-8a93a3874d93\"}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:35:59.650Z\"}",
              | "loadtime":"QK447zEwAADlgyUA"
              |},
              |{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.classPerformance.accessed",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://w3id.org/xapi/seriousgames/verbs/accessed\",\"display\":{\"en-US\":\"Class Performance Accessed\"}},\"object\":{\"id\":\"http://alefeducation.com/class-performance-page\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://alefeducation.com/xapi/activities/analytics\"}},\"context\":{\"contextActivities\":{\"parent\":[{\"id\":\"http://alefeducation.com/class-performance-table\"},{\"id\":\"http://alefeducation.com/learning-path/c672c131-e655-40ef-b413-2a42be895a63\"}],\"grouping\":[{\"id\":\"http://alefeducation.com/curriculum/392027/grade/596550/subject/352071\"}],\"category\":[{\"id\":\"http://alefeducation.com/classPerformance\"}]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T05:58:04.577Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36\",\"outsideOfSchool\": true,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{\"id\":\"6e796516-72d5-4b71-9923-cac39b8d6109\"},\"subject\":{\"id\":\"f6523b83-c0e7-47f1-a276-15b384389bc0\",\"name\":\"Arabic\"},\"section\":{\"id\":\"9095e929-b8c3-4da9-829f-f4353edc1cfa\"}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:36:15.650Z\"}",
              | "loadtime":"QFDTSYoTAADlgyUA"
              |},
              |{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.platform.login",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/login/\",\"display\":{\"en-US\":\"Log In\"}},\"object\":{\"id\":\"http://alefeducation.com/login\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T15:30:59.572Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\",\"outsideOfSchool\": true,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T16:07:15.650Z\"}",
              | "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.classPerformance.accessed",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://w3id.org/xapi/seriousgames/verbs/accessed\",\"display\":{\"en-US\":\"Class Performance Accessed\"}},\"object\":{\"id\":\"http://alefeducation.com/class-performance-page\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://alefeducation.com/xapi/activities/analytics\"}},\"context\":{\"contextActivities\":{\"parent\":[{\"id\":\"http://alefeducation.com/class-performance-table\"},{\"id\":\"http://alefeducation.com/learning-path/c672c131-e655-40ef-b413-2a42be895a63\"}],\"grouping\":[{\"id\":\"http://alefeducation.com/curriculum/392027/grade/596550/subject/352071\"}],\"category\":[{\"id\":\"http://alefeducation.com/classPerformance\"}]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T05:58:04.577Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36\",\"outsideOfSchool\": true,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{\"id\":\"6e796516-72d5-4b71-9923-cac39b8d6109\"},\"subject\":{\"id\":\"f6523b83-c0e7-47f1-a276-15b384389bc0\",\"name\":\"Arabic\"},\"section\":{\"id\":\"9095e929-b8c3-4da9-829f-f4353edc1cfa\"}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T16:07:17.650Z\"}",
              | "loadtime":"QFDTSYoTAADlgyUA"
              |}
              |]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val redshiftSink = sinks.find(_.name == RedshiftTeacherActivitiesSink)
          val redshiftDf = redshiftSink.head.output
          assert(redshiftDf.count == 5)
          val record1 = redshiftDf.filter("fta_start_time = '2019-04-23 15:30:59.65'").first()
          assertRow[Double](record1, "fta_time_spent", 300)
          assertRow[String](record1, "teacher_uuid", "afd49ed3-f105-4a56-a908-3b1301c3edbb")
          assertRow[String](record1, "school_uuid", "1cdba148-3e14-4abd-8fee-6829537a25c6")
          assertRow[String](record1, "grade_uuid", DEFAULT_ID)
          assertRow[String](record1, "section_uuid", DEFAULT_ID)
          assertRow[String](record1, "subject_uuid", DEFAULT_ID)
          assertRow[String](record1, "fta_event_type", "teacher.platform.login")
          assertRow[String](record1, "fta_prev_event_type", null)
          assertRow[String](record1, "fta_next_event_type", "teacher.class.selected")
          assertTimestampRow(record1, "fta_end_time", "2019-04-23 15:35:59.65")
          assertRow[Int](record1, "fta_date_dw_id", 20190423)
          assertRow[Boolean](record1, "fta_outside_of_school", true)

          val record2 = redshiftDf.filter("fta_start_time = '2019-04-23 15:35:59.65'").first()
          assertRow[Double](record2, "fta_time_spent", 16)
          assertRow[String](record2, "teacher_uuid", "afd49ed3-f105-4a56-a908-3b1301c3edbb")
          assertRow[String](record2, "school_uuid", "1cdba148-3e14-4abd-8fee-6829537a25c6")
          assertRow[String](record2, "grade_uuid", "462dfaa3-0088-4016-9974-5bf55df25f23")
          assertRow[String](record2, "section_uuid", "5a94144a-ae63-45b5-bcbc-8a93a3874d93")
          assertRow[String](record2, "subject_uuid", "d79aeb6b-537b-4051-9a8d-45d4957b6c34")
          assertRow[String](record2, "fta_event_type", "teacher.class.selected")
          assertRow[String](record2, "fta_prev_event_type", "teacher.platform.login")
          assertRow[String](record2, "fta_next_event_type", "teacher.classPerformance.accessed")
          assertTimestampRow(record2, "fta_end_time", "2019-04-23 15:36:15.65")
          assertRow[Int](record2, "fta_date_dw_id", 20190423)
          assertRow[Boolean](record2, "fta_outside_of_school", true)

          val record3 = redshiftDf.filter("fta_start_time = '2019-04-23 16:07:15.65'").first()
          assertRow[Double](record3, "fta_time_spent", 2)
          assertRow[String](record3, "teacher_uuid", "afd49ed3-f105-4a56-a908-3b1301c3edbb")
          assertRow[String](record3, "school_uuid", "1cdba148-3e14-4abd-8fee-6829537a25c6")
          assertRow[String](record3, "grade_uuid", DEFAULT_ID)
          assertRow[String](record3, "section_uuid", DEFAULT_ID)
          assertRow[String](record3, "subject_uuid", DEFAULT_ID)
          assertRow[String](record3, "fta_event_type", "teacher.platform.login")
          assertRow[String](record3, "fta_prev_event_type", "teacher.classPerformance.accessed")
          assertRow[String](record3, "fta_next_event_type", "teacher.classPerformance.accessed")
          assertTimestampRow(record3, "fta_end_time", "2019-04-23 16:07:17.65")
          assertRow[Int](record3, "fta_date_dw_id", 20190423)
          assertRow[Boolean](record3, "fta_outside_of_school", true)

          val invalidSink = sinks.find(_.name == ParquetInvalidTeacherXApiV2Sink)
          assert(invalidSink.isEmpty)
        }
      )
    }
  }

  test("test login event with invalid actor.account.name") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetTeacherXApiV2Source,
          value =
            """
              |[{
              |  "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              |  "eventType": "teacher.platform.login",
              |  "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbbINVALID\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/login/\",\"display\":{\"en-US\":\"Log In\"}},\"object\":{\"id\":\"http://alefeducation.com/login\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T15:30:59.572Z.........................................................................................................................................................................\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\",\"outsideOfSchool\": false,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:30:59.650Z\"}",
              |  "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.platform.logout",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbbINVALID\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/logout/\",\"display\":{\"en-US\":\"Log Out\"}},\"object\":{\"id\":\"http://alefeducation.com/logout\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T05:45:30.992Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36\",\"outsideOfSchool\": false,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:31:02.650Z\"}",
              | "loadtime":"wH+Z1NoSAADlgyUA"
              |}]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetSink = sinks.find(_.name == ParquetTeacherXApiV2Source)
          assert(parquetSink.size == 1)
          val parquetDf = parquetSink.head.output
          assert(parquetDf.count == 2)

          val redshiftSink = sinks.find(_.name == RedshiftTeacherActivitiesSink)
          assert(redshiftSink.isEmpty)

          val invalidSink = sinks.find(_.name == ParquetInvalidTeacherXApiV2Sink)
          val invalidDf = invalidSink.head.output
          assert(invalidDf.count() == 2)
        }
      )
    }
  }

  test("test login event with invalid actor.account.homePage") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetTeacherXApiV2Source,
          value =
            """
              |[{
              |  "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              |  "eventType": "teacher.platform.login",
              |  "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/this_is_long_text_more_than_100_symbols_this_is_long_text_more_than_100_symbols_this_is_long_text_more_than_100_symbols\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/login/\",\"display\":{\"en-US\":\"Log In\"}},\"object\":{\"id\":\"http://alefeducation.com/login\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T15:30:59.572Z.........................................................................................................................................................................\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\",\"outsideOfSchool\": false,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:30:59.650Z\"}",
              |  "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.platform.logout",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/logout/\",\"display\":{\"en-US\":\"Log Out\"}},\"object\":{\"id\":\"http://alefeducation.com/logout\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T05:45:30.992Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36\",\"outsideOfSchool\": false,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:31:02.650Z\"}",
              | "loadtime":"wH+Z1NoSAADlgyUA"
              |}]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetSink = sinks.find(_.name == ParquetTeacherXApiV2Source)
          assert(parquetSink.size == 1)
          val parquetDf = parquetSink.head.output
          assert(parquetDf.count == 2)

          val redshiftSink = sinks.find(_.name == RedshiftTeacherActivitiesSink)
          assert(redshiftSink.size == 1)

          val invalidSink = sinks.find(_.name == ParquetInvalidTeacherXApiV2Sink)
          val invalidDf = invalidSink.head.output
          assert(invalidDf.count() == 1)
        }
      )
    }
  }

  test("test login event with invalid actor.objectType") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetTeacherXApiV2Source,
          value =
            """
              |[{
              |  "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              |  "eventType": "teacher.platform.login",
              |  "body":"{\"actor\":{\"objectType\":\"Agent_Agent_Agent_Agent_Agent_Agent_Agent_Agent_Agent_Agent_Agent_Agent_Agent_Agent_Agent_Agent_Agent_Agent_Agent_Agent_Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/login/\",\"display\":{\"en-US\":\"Log In\"}},\"object\":{\"id\":\"http://alefeducation.com/login\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T15:30:59.572Z.........................................................................................................................................................................\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\",\"outsideOfSchool\": false,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:30:59.650Z\"}",
              |  "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.platform.logout",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/logout/\",\"display\":{\"en-US\":\"Log Out\"}},\"object\":{\"id\":\"http://alefeducation.com/logout\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T05:45:30.992Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36\",\"outsideOfSchool\": false,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:31:02.650Z\"}",
              | "loadtime":"wH+Z1NoSAADlgyUA"
              |}]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetSink = sinks.find(_.name == ParquetTeacherXApiV2Source)
          assert(parquetSink.size == 1)
          val parquetDf = parquetSink.head.output
          assert(parquetDf.count == 2)

          val redshiftSink = sinks.find(_.name == RedshiftTeacherActivitiesSink)
          assert(redshiftSink.size == 1)

          val invalidSink = sinks.find(_.name == ParquetInvalidTeacherXApiV2Sink)
          val invalidDf = invalidSink.head.output
          assert(invalidDf.count() == 1)
        }
      )
    }
  }

  test("test login event with invalid body.verb.id") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetTeacherXApiV2Source,
          value =
            """
              |[{
              |  "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              |  "eventType": "teacher.platform.login",
              |  "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/login/invalid_invalid_invalid_invalid_invalid_invalid_invalid_invalid_invalid_invalid_invalid_invalid\",\"display\":{\"en-US\":\"Log In\"}},\"object\":{\"id\":\"http://alefeducation.com/login\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T15:30:59.572Z.........................................................................................................................................................................\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\",\"outsideOfSchool\": false,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:30:59.650Z\"}",
              |  "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.platform.logout",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/logout/\",\"display\":{\"en-US\":\"Log Out\"}},\"object\":{\"id\":\"http://alefeducation.com/logout\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T05:45:30.992Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36\",\"outsideOfSchool\": false,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:31:02.650Z\"}",
              | "loadtime":"wH+Z1NoSAADlgyUA"
              |}]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetSink = sinks.find(_.name == ParquetTeacherXApiV2Source)
          assert(parquetSink.size == 1)
          val parquetDf = parquetSink.head.output
          assert(parquetDf.count == 2)

          val redshiftSink = sinks.find(_.name == RedshiftTeacherActivitiesSink)
          assert(redshiftSink.size == 1)

          val invalidSink = sinks.find(_.name == ParquetInvalidTeacherXApiV2Sink)
          val invalidDf = invalidSink.head.output
          assert(invalidDf.count() == 1)
        }
      )
    }
  }

  test("test login event with invalid body.verb.display") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetTeacherXApiV2Source,
          value =
            """
              |[{
              |  "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              |  "eventType": "teacher.platform.login",
              |  "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/login/\",\"display\":{\"en-US\":\"Log In INVALID INVALID INVALID INVALID INVALID INVALID INVALID INVALID INVALID INVALID INVALID INVALID INVALID\"}},\"object\":{\"id\":\"http://alefeducation.com/login\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T15:30:59.572Z.........................................................................................................................................................................\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\",\"outsideOfSchool\": false,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:30:59.650Z\"}",
              |  "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.platform.logout",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/logout/\",\"display\":{\"en-US\":\"Log Out\"}},\"object\":{\"id\":\"http://alefeducation.com/logout\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T05:45:30.992Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36\",\"outsideOfSchool\": false,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:31:02.650Z\"}",
              | "loadtime":"wH+Z1NoSAADlgyUA"
              |}]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetSink = sinks.find(_.name == ParquetTeacherXApiV2Source)
          assert(parquetSink.size == 1)
          val parquetDf = parquetSink.head.output
          assert(parquetDf.count == 2)

          val redshiftSink = sinks.find(_.name == RedshiftTeacherActivitiesSink)
          assert(redshiftSink.size == 1)

          val invalidSink = sinks.find(_.name == ParquetInvalidTeacherXApiV2Sink)
          val invalidDf = invalidSink.head.output
          assert(invalidDf.count() == 1)
        }
      )
    }
  }

  test("test login event with invalid object.definition.type") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetTeacherXApiV2Source,
          value =
            """
              |[{
              |  "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              |  "eventType": "teacher.platform.login",
              |  "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/login/\",\"display\":{\"en-US\":\"Log In\"}},\"object\":{\"id\":\"http://alefeducation.com/login\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page_invalid_invalid_invalid_invalid_invalid_invalid_invalid_invalid_invalid_invalid_invalid_invalid_invalid_invalid_invalid\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T15:30:59.572Z.........................................................................................................................................................................\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\",\"outsideOfSchool\": false,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:30:59.650Z\"}",
              |  "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.platform.logout",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/logout/\",\"display\":{\"en-US\":\"Log Out\"}},\"object\":{\"id\":\"http://alefeducation.com/logout\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T05:45:30.992Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36\",\"outsideOfSchool\": false,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:31:02.650Z\"}",
              | "loadtime":"wH+Z1NoSAADlgyUA"
              |}]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetSink = sinks.find(_.name == ParquetTeacherXApiV2Source)
          assert(parquetSink.size == 1)
          val parquetDf = parquetSink.head.output
          assert(parquetDf.count == 2)

          val redshiftSink = sinks.find(_.name == RedshiftTeacherActivitiesSink)
          assert(redshiftSink.size == 1)

          val invalidSink = sinks.find(_.name == ParquetInvalidTeacherXApiV2Sink)
          val invalidDf = invalidSink.head.output
          assert(invalidDf.count() == 1)
        }
      )
    }
  }

  test("test login event with invalid object.definition.name") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetTeacherXApiV2Source,
          value =
            """
              |[{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.platform.login",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/login/\",\"display\":{\"en-US\":\"Log In\"}},\"object\":{\"id\":\"http://alefeducation.com/login\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\",\"name\":{\"en-US\":\"6A_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID6A_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID6A_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID6A_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID_INVALID\"}}},\"context\":{\"contextActivities\":{\"parent\":[],\"grouping\":[],\"category\":[]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T15:30:59.572Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\",\"outsideOfSchool\": true,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{},\"subject\":{},\"section\":{}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:30:59.650Z\"}",
              | "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              | "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013",
              | "eventType": "teacher.class.selected",
              | "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com\",\"name\":\"afd49ed3-f105-4a56-a908-3b1301c3edbb\"}},\"verb\":{\"id\":\"http://id.tincanapi.com/verb/selected\",\"display\":{\"en-US\":\"class selected\"}},\"object\":{\"id\":\"http://alefeducation.com/class.filter\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"https://w3id.org/xapi/seriousgames/activity-types/menu\",\"name\":{\"en-US\":\"6A\"}}},\"context\":{\"contextActivities\":{\"parent\":[{\"id\":\"http://alefeducation.com/#/analytics/tab/classPerformance\"},{\"id\":\"http://alefeducation.com/learning-path/88b387ac-5f56-41d2-b7bd-ca1685bb3a8e\"}],\"grouping\":[{\"id\":\"http://alefeducation.com/curriculum/392027/grade/596550/subject/571671\"}],\"category\":[{\"id\":\"http://alefeducation.com/class\"}]},\"extensions\":{\"http://alefeducation.com\":{\"role\":\"TEACHER\",\"timestampLocal\":\"2019-04-23T14:43:10.785Z\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36\",\"outsideOfSchool\": true,\"fromMoeSchool\":false,\"tenant\":{\"id\":\"93e4949d-7eff-4707-9201-dac917a5e013\",\"school\":{\"id\":\"1cdba148-3e14-4abd-8fee-6829537a25c6\",\"grade\":{\"id\":\"462dfaa3-0088-4016-9974-5bf55df25f23\"},\"subject\":{\"id\":\"d79aeb6b-537b-4051-9a8d-45d4957b6c34\",\"name\":\"Math\"},\"section\":{\"id\":\"5a94144a-ae63-45b5-bcbc-8a93a3874d93\"}}}}}},\"result\":{},\"timestamp\":\"2019-04-23T15:35:59.650Z\"}",
              | "loadtime":"QK447zEwAADlgyUA"
              |}]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetSink = sinks.find(_.name == ParquetTeacherXApiV2Source)
          assert(parquetSink.size == 1)
          val parquetDf = parquetSink.head.output
          assert(parquetDf.count == 2)

          val redshiftSink = sinks.find(_.name == RedshiftTeacherActivitiesSink)
          val redshiftDf = redshiftSink.head.output
          assert(redshiftDf.count == 1)

          val invalidSink = sinks.find(_.name == ParquetInvalidTeacherXApiV2Sink)
          val invalidDf = invalidSink.head.output
          assert(invalidDf.count() == 1)
        }
      )
    }
  }
}
