package com.alefeducation.facts

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.facts.xapi.StudentXAPITransformer
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers._

class StudentXApiAggregationSpec extends SparkSuite {

  trait Setup {
    implicit val transformer = new StudentXAPITransformer(StudentXapiV2Service, spark)
  }

  test("student xapi events") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetStudentXApiV2Source,
          value =
            """
              |[
              |{
              |   "eventType":"student.platform.login",
              |   "tenantId":"21ce4d98-8286-4f7e-b122-03b98a5f3b2e",
              |   "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/STUDENT\",\"name\":\"42782fc0-ceaf-4792-b360-dede708a6999\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/login/\",\"display\":{\"en-US\":\"Log In\"}},\"object\":{\"id\":\"http://alefeducation.com/login\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[]},\"extensions\":{\"http://alefeducation.com\":{\"timestampLocal\":\"Tue Jul 23 2019 09:52:46 GMT+0400(Gulf Standard Time))\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36\",\"outsideOfSchool\":true,\"tenant\":{\"id\":\"21ce4d98-8286-4f7e-b122-03b98a5f3b2e\",\"school\":{\"id\":\"3b4775c8-2727-4e29-abfc-3d2cdfe92311\",\"name\":\"Turtle\",\"academicYearId\":\"b52342ad-8205-11e9-97db-0242ac110006\",\"grade\":{\"id\":\"ab0e0adc-2082-4ac7-9895-30bd7909ba2f\",\"name\":\"7\"},\"section\":{\"id\":\"04d1f4d8-47e8-4f3d-a1f0-c233fb6eeec7\",\"name\":\"7-A\"}}}}}},\"result\":{},\"timestamp\":\"2019-07-23T05:52:46.876Z\"}",
              |   "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              |   "eventType":"student.mloLearnPage.content.initialized",
              |   "tenantId":"21ce4d98-8286-4f7e-b122-03b98a5f3b2e",
              |   "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/STUDENT\",\"name\":\"42782fc0-ceaf-4792-b360-dede708a6999\"}},\"verb\":{\"id\":\"http://adlnet.gov/expapi/verbs/initialized\",\"display\":{\"en-US\":\"initialized\"}},\"object\":{\"id\":\"http://alefeducation.com/experience/7f0eb92f-e676-4e00-878b-964f517b2925\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://alefeducation.com/xapi/activities/experience\",\"name\":{\"en-US\":\"Last Look\"}}},\"context\":{\"contextActivities\":{\"parent\":[{\"id\":\"http://alefeducation.com/learning-path/c6dd42c9-fbbf-46b6-87e7-3c388f629a95\"},{\"id\":\"http://alefeducation.com/lesson/e8995472-ea4f-4c40-a7e0-fc982cc4c3b6\",\"definition\":{\"name\":{\"en-US\":\"Types of Rocks\"}}}],\"grouping\":[{\"id\":\"http://alefeducation.com/curriculum/392027/grade/596550/subject/742980\"}]},\"extensions\":{\"http://alefeducation.com\":{\"attempt\":2,\"experienceId\":\"271b3762-6e66-4e54-95e1-cb2d02a318ae\",\"learningSessionId\":\"6097a89f-3b71-4dd7-9a33-948363effb23\",\"timestampLocal\":\"Tue Jul 23 2019 09:53:29 GMT+0400 (Gulf Standard Time)----Tue Jul 23 2019 09:52:46 GMT+0400 (Gulf Standard Time\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36(KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36\",\"outsideOfSchool\":true,\"lessonPosition\":1,\"tenant\":{\"id\":\"21ce4d98-8286-4f7e-b122-03b98a5f3b2e\",\"school\":{\"academicCalendar\":{\"id\":\"d8308c71-0b33-4d4c-aa23-59f3c5e1cb2d\",\"currentPeriod\":{\"teachingPeriodId\":\"a56f6894-9a17-4c67-92bb-ec7b3445e9af\",\"teachingPeriodTitle\":\"Period 1\"}},\"id\":\"3b4775c8-2727-4e29-abfc-3d2cdfe92311\",\"name\":\"Turtle\",\"academicYearId\":\"b52342ad-8205-11e9-97db-0242ac110006\",\"grade\":{\"id\":\"ab0e0adc-2082-4ac7-9895-30bd7909ba2f\",\"name\":\"7\"},\"subject\":{\"id\":\"a010cbc3-0f61-4b9f-84ed-023e112c33c9\"},\"section\":{\"id\":\"04d1f4d8-47e8-4f3d-a1f0-c233fb6eeec7\",\"name\":\"7-A\"}}}}}},\"result\":{},\"timestamp\":\"2019-07-23T05:53:29.252Z\"}",
              |   "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              |   "eventType":"student.mloLearnPage.content.seeked",
              |   "tenantId":"21ce4d98-8286-4f7e-b122-03b98a5f3b2e",
              |   "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/STUDENT\",\"name\":\"42782fc0-ceaf-4792-b360-dede708a6999\"}},\"verb\":{\"id\":\"https://w3id.org/xapi/video/verbs/seeked\",\"display\":{\"en-US\":\"seeked\"}},\"object\":{\"id\":\"http://alefeducation.com/content/7f0eb92f-e676-4e00-878b-964f517b2925\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"https://w3id.org/xapi/video/activity-type/video\",\"name\":{\"en-US\":\"Last Look\"},\"extensions\":{\"fromTime\":1.872894,\"toTime\":92.781287}}},\"context\":{\"contextActivities\":{\"parent\":[{\"id\":\"http://alefeducation.com/learning-path/c6dd42c9-fbbf-46b6-87e7-3c388f629a95\"},{\"id\":\"http://alefeducation.com/lesson/e8995472-ea4f-4c40-a7e0-fc982cc4c3b6\",\"definition\":{\"name\":{\"en-US\":\"Types of Rocks\"}}},{\"id\":\"http://alefeducation.com/experience/271b3762-6e66-4e54-95e1-cb2d02a318ae\"}],\"grouping\":[{\"id\":\"http://alefeducation.com/curriculum/392027/grade/596550/subject/742980\"}]},\"extensions\":{\"http://alefeducation.com\":{\"attempt\":2,\"experienceId\":\"271b3762-6e66-4e54-95e1-cb2d02a318ae\",\"learningSessionId\":\"6097a89f-3b71-4dd7-9a33-948363effb23\",\"timestampLocal\":\"Tue Jul 23 2019 09:53:34 GMT+0400(Gulf Standard Time)\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36\",\"outsideOfSchool\":true,\"fromTime\":1.872894,\"toTime\":92.781287,\"tenant\":{\"id\":\"21ce4d98-8286-4f7e-b122-03b98a5f3b2e\",\"school\":{\"academicCalendar\":{\"id\":\"d8308c71-0b33-4d4c-aa23-59f3c5e1cb2d\",\"currentPeriod\":{\"teachingPeriodId\":\"a56f6894-9a17-4c67-92bb-ec7b3445e9af\",\"teachingPeriodTitle\":\"Period 1\"}},\"id\":\"3b4775c8-2727-4e29-abfc-3d2cdfe92311\",\"name\":\"Turtle\",\"academicYearId\":\"b52342ad-8205-11e9-97db-0242ac110006\",\"grade\":{\"id\":\"ab0e0adc-2082-4ac7-9895-30bd7909ba2f\",\"name\":\"7\"},\"subject\":{\"id\":\"a010cbc3-0f61-4b9f-84ed-023e112c33c9\",\"name\":\"Science\"},\"section\":{\"id\":\"04d1f4d8-47e8-4f3d-a1f0-c233fb6eeec7\",\"name\":\"7-A\"}}}}}},\"result\":{},\"timestamp\":\"2019-07-23T05:53:34.239Z\"}",
              |   "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              |   "eventType":"student.mloLearnPage.content.completed",
              |   "tenantId":"21ce4d98-8286-4f7e-b122-03b98a5f3b2e",
              |   "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/STUDENT\",\"name\":\"42782fc0-ceaf-4792-b360-dede708a6999\"}},\"verb\":{\"id\":\"http://adlnet.gov/expapi/verbs/completed\",\"display\":{\"en-US\":\"completed\"}},\"object\":{\"id\":\"http://alefeducation.com/experience/541e9970-7f99-4302-9f6d-dfb25d53c936\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://alefeducation.com/xapi/activities/experience\",\"name\":{\"en-US\":\"The Big Idea\"}}},\"context\":{\"contextActivities\":{\"parent\":[{\"id\":\"http://alefeducation.com/learning-path/c6dd42c9-fbbf-46b6-87e7-3c388f629a95\"},{\"id\":\"http://alefeducation.com/lesson/e8995472-ea4f-4c40-a7e0-fc982cc4c3b6\",\"definition\":{\"name\":{\"en-US\":\"Types of Rocks\"}}}],\"grouping\":[{\"id\":\"http://alefeducation.com/curriculum/392027/grade/596550/subject/742980\"}]},\"extensions\":{\"http://alefeducation.com\":{\"attempt\":2,\"experienceId\":\"67f79f36-8bdb-41a2-a2d1-6e3178e59bbc\",\"learningSessionId\":\"6097a89f-3b71-4dd7-9a33-948363effb23\",\"timestampLocal\":\"Tue Jul 23 2019 09:56:44 GMT+0400 (Gulf Standard Time)\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36\",\"outsideOfSchool\":true,\"tenant\":{\"id\":\"21ce4d98-8286-4f7e-b122-03b98a5f3b2e\",\"school\":{\"academicCalendar\":{\"id\":\"d8308c71-0b33-4d4c-aa23-59f3c5e1cb2d\",\"currentPeriod\":{\"teachingPeriodId\":\"a56f6894-9a17-4c67-92bb-ec7b3445e9af\",\"teachingPeriodTitle\":\"Period 1\"}},\"id\":\"3b4775c8-2727-4e29-abfc-3d2cdfe92311\",\"name\":\"Turtle\",\"academicYearId\":\"b52342ad-8205-11e9-97db-0242ac110006\",\"grade\":{\"id\":\"ab0e0adc-2082-4ac7-9895-30bd7909ba2f\",\"name\":\"7\"},\"subject\":{\"id\":\"a010cbc3-0f61-4b9f-84ed-023e112c33c9\"},\"section\":{\"id\":\"04d1f4d8-47e8-4f3d-a1f0-c233fb6eeec7\",\"name\":\"7-A\"}}}}}},\"result\":{},\"timestamp\":\"2019-07-23T05:56:44.557Z\"}",
              |   "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              |   "eventType":"student.mloLearnPage.content.initialized",
              |   "tenantId":"21ce4d98-8286-4f7e-b122-03b98a5f3b2e",
              |   "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/STUDENT\",\"name\":\"42782fc0-ceaf-4792-b360-dede708a6999\"}},\"verb\":{\"id\":\"http://adlnet.gov/expapi/verbs/initialized\",\"display\":{\"en-US\":\"initialized\"}},\"object\":{\"id\":\"http://alefeducation.com/experience/2f8e9ba5-f154-424a-bd3f-06621c13fc0c\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://alefeducation.com/xapi/activities/experience\",\"name\":{\"en-US\":\"Check My Understanding 1\"}}},\"context\":{\"contextActivities\":{\"parent\":[{\"id\":\"http://alefeducation.com/learning-path/c6dd42c9-fbbf-46b6-87e7-3c388f629a95\"},{\"id\":\"http://alefeducation.com/lesson/e8995472-ea4f-4c40-a7e0-fc982cc4c3b6\",\"definition\":{\"name\":{\"en-US\":\"Types of Rocks\"}}}],\"grouping\":[{\"id\":\"http://alefeducation.com/curriculum/392027/grade/596550/subject/742980\"}]},\"extensions\":{\"http://alefeducation.com\":{\"attempt\":2,\"experienceId\":\"e134c827-fe4f-4e6f-9c2d-38a5594a1478\",\"learningSessionId\":\"6097a89f-3b71-4dd7-9a33-948363effb23\",\"timestampLocal\":\"Tue Jul 23 2019 09:56:52 GMT+0400 (Gulf Standard Time)\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36\",\"outsideOfSchool\":true,\"tenant\":{\"id\":\"21ce4d98-8286-4f7e-b122-03b98a5f3b2e\",\"school\":{\"academicCalendar\":{\"id\":\"d8308c71-0b33-4d4c-aa23-59f3c5e1cb2d\",\"currentPeriod\":{\"teachingPeriodId\":\"a56f6894-9a17-4c67-92bb-ec7b3445e9af\",\"teachingPeriodTitle\":\"Period 1\"}},\"id\":\"3b4775c8-2727-4e29-abfc-3d2cdfe92311\",\"name\":\"Turtle\",\"academicYearId\":\"b52342ad-8205-11e9-97db-0242ac110006\",\"grade\":{\"id\":\"ab0e0adc-2082-4ac7-9895-30bd7909ba2f\",\"name\":\"7\"},\"subject\":{\"id\":\"a010cbc3-0f61-4b9f-84ed-023e112c33c9\"},\"section\":{\"id\":\"04d1f4d8-47e8-4f3d-a1f0-c233fb6eeec7\",\"name\":\"7-A\"}}}}}},\"result\":{},\"timestamp\":\"2019-07-23T05:56:52.379Z\"}",
              |   "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              |   "eventType":"student.platform.logout",
              |   "tenantId":"21ce4d98-8286-4f7e-b122-03b98a5f3b2e",
              |   "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/STUDENT\",\"name\":\"42782fc0-ceaf-4792-b360-dede708a6999\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/logout/\",\"display\":{\"en-US\":\"Log Out\"}},\"object\":{\"id\":\"http://alefeducation.com/logout\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[]},\"extensions\":{\"http://alefeducation.com\":{\"timestampLocal\":\"Tue Jul 23 2019 13:46:41 GMT+0400 (Gulf Standard Time)\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36\",\"outsideOfSchool\":true,\"tenant\":{\"id\":\"21ce4d98-8286-4f7e-b122-03b98a5f3b2e\",\"school\":{\"id\":\"3b4775c8-2727-4e29-abfc-3d2cdfe92311\",\"name\":\"Turtle\",\"academicYearId\":\"b52342ad-8205-11e9-97db-0242ac110006\",\"grade\":{\"id\":\"ab0e0adc-2082-4ac7-9895-30bd7909ba2f\",\"name\":\"7\"},\"section\":{\"id\":\"04d1f4d8-47e8-4f3d-a1f0-c233fb6eeec7\",\"name\":\"7=A\"}}}}}},\"result\":{},\"timestamp\":\"2019-07-23T05:57:41.343Z\"}",
              |   "loadtime":"APm21s0yAADlgyUA"
              |}
              |]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetSink = sinks.find(_.name == ParquetStudentXApiV2Source)
          assert(parquetSink.size == 1)
          val parquetDf = parquetSink.head.output
          assert(parquetDf.count == 6)
          assert(parquetSink.head.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate,eventType"))
          val redshiftSink = sinks.find(_.name == RedshiftStudentActivitiesSink)
          assert(redshiftSink.size == 1)

          val redshiftDf = redshiftSink.head.output
          assert(redshiftDf.count == 6)

          val redshiftLogin = redshiftSink.head.output.filter("fsta_event_type = 'student.platform.login'")
          assert[String](redshiftLogin, "fsta_academic_calendar_id", "DEFAULT_ID")
          assert[String](redshiftLogin, "fsta_teaching_period_id", "DEFAULT_ID")
          assert[String](redshiftLogin, "fsta_teaching_period_title", null)
          assert[String](redshiftLogin, "tenant_uuid", "21ce4d98-8286-4f7e-b122-03b98a5f3b2e")
          assert[String](redshiftLogin, "school_uuid", "3b4775c8-2727-4e29-abfc-3d2cdfe92311")
          assert[String](redshiftLogin, "section_uuid", "04d1f4d8-47e8-4f3d-a1f0-c233fb6eeec7")

          val redshiftLogout = redshiftSink.head.output.filter("fsta_event_type = 'student.platform.logout'")
          assert[Double](redshiftLogout, "fsta_date_dw_id", 20190723)
          assert[BigDecimal](redshiftLogout, "fsta_score_max", -1.0)
          assert[Boolean](redshiftLogout, "fsta_outside_of_school", true)
          assert[String](redshiftLogout, "fsta_start_time", "2019-07-23 05:57:41.343")
          assert[String](redshiftLogout, "fsta_exp_id", "DEFAULT_ID")
          assert[String](redshiftLogout, "student_uuid", "42782fc0-ceaf-4792-b360-dede708a6999")
          assert[String](redshiftLogout, "school_uuid", "3b4775c8-2727-4e29-abfc-3d2cdfe92311")
          assert[String](redshiftLogout, "fsta_verb_id", "https://brindlewaye.com/xAPITerms/verbs/logout/")
          assert[Boolean](redshiftLogout, "fsta_is_completion_node", false)
          assert[String](redshiftLogout, "fsta_academic_calendar_id", "DEFAULT_ID")
          assert[String](redshiftLogout, "fsta_time_spent", null)
          assert[String](redshiftLogout, "section_uuid", "04d1f4d8-47e8-4f3d-a1f0-c233fb6eeec7")
          assert[String](redshiftLogout, "fsta_verb_display", "{\"en-US\":\"Log Out\"}")
          assert[String](redshiftLogout, "tenant_uuid", "21ce4d98-8286-4f7e-b122-03b98a5f3b2e")
          assert[String](redshiftLogout, "academic_year_uuid", "b52342ad-8205-11e9-97db-0242ac110006")
          assert[String](redshiftLogout, "grade_uuid", "ab0e0adc-2082-4ac7-9895-30bd7909ba2f")

          val redshiftStudentXapiDf =
            redshiftSink.head.output.filter("fsta_event_type = 'student.mloLearnPage.content.initialized'")
          assert[String](redshiftStudentXapiDf, "fsta_object_type", "Activity")
          assert[String](redshiftStudentXapiDf,
                         "fsta_object_id",
                         "http://alefeducation.com/experience/7f0eb92f-e676-4e00-878b-964f517b2925")
          assert[String](redshiftStudentXapiDf, "fsta_actor_account_homepage", "http://alefeducation.com/users/STUDENT")
          assert[String](redshiftStudentXapiDf, "fsta_academic_calendar_id", "d8308c71-0b33-4d4c-aa23-59f3c5e1cb2d")
          assert[String](redshiftStudentXapiDf, "fsta_teaching_period_id", "a56f6894-9a17-4c67-92bb-ec7b3445e9af")
          assert[String](redshiftStudentXapiDf, "fsta_teaching_period_title", "Period 1")
          assert[String](redshiftStudentXapiDf, "tenant_uuid", "21ce4d98-8286-4f7e-b122-03b98a5f3b2e")
          assert[String](redshiftStudentXapiDf, "school_uuid", "3b4775c8-2727-4e29-abfc-3d2cdfe92311")
          assert[String](redshiftStudentXapiDf, "student_uuid", "42782fc0-ceaf-4792-b360-dede708a6999")
          assert[String](redshiftStudentXapiDf, "grade_uuid", "ab0e0adc-2082-4ac7-9895-30bd7909ba2f")
          assert[String](redshiftStudentXapiDf, "section_uuid", "04d1f4d8-47e8-4f3d-a1f0-c233fb6eeec7")
          assert[String](redshiftStudentXapiDf, "subject_uuid", "a010cbc3-0f61-4b9f-84ed-023e112c33c9")
          assert[String](redshiftStudentXapiDf, "academic_year_uuid", "b52342ad-8205-11e9-97db-0242ac110006")
          assert[String](redshiftStudentXapiDf, "fsta_actor_object_type", "Agent")
          assert[String](redshiftStudentXapiDf, "fsta_verb_id", "http://adlnet.gov/expapi/verbs/initialized")
          assert[String](redshiftStudentXapiDf, "fsta_start_time", "2019-07-23 05:53:29.252")
          assert[String](redshiftStudentXapiDf, "fsta_end_time", "2019-07-23 05:53:34.239")
          assert[Double](redshiftStudentXapiDf, "fsta_time_spent", 5)
          assert(redshiftStudentXapiDf.first.getAs[String]("fsta_timestamp_local").length === EndIndexTimestampLocal)

          val redshiftSeekedXApiDf =
            redshiftSink.head.output.filter("fsta_event_type = 'student.mloLearnPage.content.seeked'")
          assert[String](redshiftStudentXapiDf, "academic_year_uuid", "b52342ad-8205-11e9-97db-0242ac110006")
          assert[String](redshiftSeekedXApiDf, "fsta_object_type", "Activity")
          assert[String](redshiftSeekedXApiDf, "fsta_object_id", "http://alefeducation.com/content/7f0eb92f-e676-4e00-878b-964f517b2925")
          assert[String](redshiftSeekedXApiDf, "fsta_verb_id", "https://w3id.org/xapi/video/verbs/seeked")
          assert[String](redshiftSeekedXApiDf, "fsta_start_time", "2019-07-23 05:53:34.239")
          assert[String](redshiftSeekedXApiDf, "fsta_end_time", "2019-07-23 05:56:44.557")
          assert[BigDecimal](redshiftSeekedXApiDf, "fsta_from_time", 1.872894)
          assert[BigDecimal](redshiftSeekedXApiDf, "fsta_to_time", 92.781287)
          assert[Double](redshiftSeekedXApiDf, "fsta_time_spent", 190)

          val redshiftCompletedXApiDf =
            redshiftSink.head.output.filter("fsta_event_type = 'student.mloLearnPage.content.completed'")
          assert[String](redshiftStudentXapiDf, "academic_year_uuid", "b52342ad-8205-11e9-97db-0242ac110006")
          assert[String](redshiftCompletedXApiDf, "fsta_object_type", "Activity")
          assert[Boolean](redshiftCompletedXApiDf, "fsta_is_completion_node", false)
          assert[Boolean](redshiftCompletedXApiDf, "fsta_is_flexible_lesson", false)
          assert[String](redshiftCompletedXApiDf,
                         "fsta_object_id",
                         "http://alefeducation.com/experience/541e9970-7f99-4302-9f6d-dfb25d53c936")
          assert[String](redshiftCompletedXApiDf, "fsta_verb_id", "http://adlnet.gov/expapi/verbs/completed")
          assert[String](redshiftCompletedXApiDf, "fsta_start_time", "2019-07-23 05:56:44.557")
          assert[String](redshiftCompletedXApiDf, "fsta_end_time", "2019-07-23 05:56:52.379")
          assert[Double](redshiftCompletedXApiDf, "fsta_time_spent", 8)
          assert[BigDecimal](redshiftCompletedXApiDf, "fsta_from_time", -1.0)
          assert[BigDecimal](redshiftCompletedXApiDf, "fsta_to_time", -1.0)
          assert[BigDecimal](redshiftCompletedXApiDf, "fsta_score_scaled", -1.0)
        }
      )
    }
  }

  test("student xapi events when flexible framework fields also coming") {
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetStudentXApiV2Source,
          value =
            """
              |[
              |{
              |   "eventType":"student.platform.login",
              |   "tenantId":"21ce4d98-8286-4f7e-b122-03b98a5f3b2e",
              |   "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/STUDENT\",\"name\":\"42782fc0-ceaf-4792-b360-dede708a6999\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/login/\",\"display\":{\"en-US\":\"Log In\"}},\"object\":{\"id\":\"http://alefeducation.com/login\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[]},\"extensions\":{\"http://alefeducation.com\":{\"timestampLocal\":\"Tue Jul 23 2019 09:52:46 GMT+0400 (Gulf Standard Time))\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36\",\"outsideOfSchool\":true,\"tenant\":{\"id\":\"21ce4d98-8286-4f7e-b122-03b98a5f3b2e\",\"school\":{\"academicCalendar\":{\"id\":\"d8308c71-0b33-4d4c-aa23-59f3c5e1cb2d\",\"currentPeriod\":{\"teachingPeriodId\":\"a56f6894-9a17-4c67-92bb-ec7b3445e9af\",\"teachingPeriodTitle\":\"Period 1\"}},\"id\":\"3b4775c8-2727-4e29-abfc-3d2cdfe92311\",\"name\":\"Turtle\",\"academicYearId\":\"b52342ad-8205-11e9-97db-0242ac110006\",\"grade\":{\"id\":\"ab0e0adc-2082-4ac7-9895-30bd7909ba2f\",\"name\":\"7\"},\"section\":{\"id\":\"04d1f4d8-47e8-4f3d-a1f0-c233fb6eeec7\",\"name\":\"7-A\"}}}}}},\"result\":{},\"timestamp\":\"2019-07-23T05:52:46.876Z\"}",
              |   "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              |   "eventType":"student.mloLearnPage.content.initialized",
              |   "tenantId":"21ce4d98-8286-4f7e-b122-03b98a5f3b2e",
              |   "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/STUDENT\",\"name\":\"42782fc0-ceaf-4792-b360-dede708a6999\"}},\"verb\":{\"id\":\"http://adlnet.gov/expapi/verbs/initialized\",\"display\":{\"en-US\":\"initialized\"}},\"object\":{\"id\":\"http://alefeducation.com/experience/7f0eb92f-e676-4e00-878b-964f517b2925\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://alefeducation.com/xapi/activities/experience\",\"name\":{\"en-US\":\"Last Look\"}}},\"context\":{\"contextActivities\":{\"parent\":[{\"id\":\"http://alefeducation.com/learning-path/c6dd42c9-fbbf-46b6-87e7-3c388f629a95\"},{\"id\":\"http://alefeducation.com/lesson/e8995472-ea4f-4c40-a7e0-fc982cc4c3b6\",\"definition\":{\"name\":{\"en-US\":\"Types of Rocks\"}}}],\"grouping\":[{\"id\":\"http://alefeducation.com/curriculum/392027/grade/596550/subject/742980\"}]},\"extensions\":{\"http://alefeducation.com\":{\"attempt\":2,\"experienceId\":\"271b3762-6e66-4e54-95e1-cb2d02a318ae\",\"learningSessionId\":\"6097a89f-3b71-4dd7-9a33-948363effb23\",\"timestampLocal\":\"Tue Jul 23 2019 09:53:29 GMT+0400 (Gulf Standard Time)----Tue Jul 23 2019 09:52:46 GMT+0400 (Gulf Standard Time\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36\",\"outsideOfSchool\":true,\"lessonPosition\":1,\"tenant\":{\"id\":\"21ce4d98-8286-4f7e-b122-03b98a5f3b2e\",\"school\":{\"academicCalendar\":{\"id\":\"d8308c71-0b33-4d4c-aa23-59f3c5e1cb2d\",\"currentPeriod\":{\"teachingPeriodId\":\"a56f6894-9a17-4c67-92bb-ec7b3445e9af\",\"teachingPeriodTitle\":\"Period 1\"}},\"id\":\"3b4775c8-2727-4e29-abfc-3d2cdfe92311\",\"name\":\"Turtle\",\"academicYearId\":\"b52342ad-8205-11e9-97db-0242ac110006\",\"grade\":{\"id\":\"ab0e0adc-2082-4ac7-9895-30bd7909ba2f\",\"name\":\"7\"},\"subject\":{\"id\":\"a010cbc3-0f61-4b9f-84ed-023e112c33c9\"},\"section\":{\"id\":\"04d1f4d8-47e8-4f3d-a1f0-c233fb6eeec7\",\"name\":\"7-A\"}}}}}},\"result\":{},\"timestamp\":\"2019-07-23T05:53:29.252Z\"}",
              |   "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              |   "eventType":"student.mloLearnPage.content.seeked",
              |   "tenantId":"21ce4d98-8286-4f7e-b122-03b98a5f3b2e",
              |   "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/STUDENT\",\"name\":\"42782fc0-ceaf-4792-b360-dede708a6999\"}},\"verb\":{\"id\":\"https://w3id.org/xapi/video/verbs/seeked\",\"display\":{\"en-US\":\"seeked\"}},\"object\":{\"id\":\"http://alefeducation.com/content/7f0eb92f-e676-4e00-878b-964f517b2925\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"https://w3id.org/xapi/video/activity-type/video\",\"name\":{\"en-US\":\"Last Look\"},\"extensions\":{\"fromTime\":1.872894,\"toTime\":92.781287}}},\"context\":{\"contextActivities\":{\"parent\":[{\"id\":\"http://alefeducation.com/learning-path/c6dd42c9-fbbf-46b6-87e7-3c388f629a95\"},{\"id\":\"http://alefeducation.com/lesson/e8995472-ea4f-4c40-a7e0-fc982cc4c3b6\",\"definition\":{\"name\":{\"en-US\":\"Types of Rocks\"}}},{\"id\":\"http://alefeducation.com/experience/271b3762-6e66-4e54-95e1-cb2d02a318ae\"}],\"grouping\":[{\"id\":\"http://alefeducation.com/curriculum/392027/grade/596550/subject/742980\"}]},\"extensions\":{\"http://alefeducation.com\":{\"attempt\":2,\"experienceId\":\"271b3762-6e66-4e54-95e1-cb2d02a318ae\",\"learningSessionId\":\"6097a89f-3b71-4dd7-9a33-948363effb23\",\"timestampLocal\":\"Tue Jul 23 2019 09:53:34 GMT+0400 (Gulf Standard Time)\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36\",\"outsideOfSchool\":true,\"fromTime\":1.872894,\"toTime\":92.781287,\"tenant\":{\"id\":\"21ce4d98-8286-4f7e-b122-03b98a5f3b2e\",\"school\":{\"academicCalendar\":{\"id\":\"d8308c71-0b33-4d4c-aa23-59f3c5e1cb2d\",\"currentPeriod\":{\"teachingPeriodId\":\"a56f6894-9a17-4c67-92bb-ec7b3445e9af\",\"teachingPeriodTitle\":\"Period 1\"}},\"id\":\"3b4775c8-2727-4e29-abfc-3d2cdfe92311\",\"name\":\"Turtle\",\"academicYearId\":\"b52342ad-8205-11e9-97db-0242ac110006\",\"grade\":{\"id\":\"ab0e0adc-2082-4ac7-9895-30bd7909ba2f\",\"name\":\"7\"},\"subject\":{\"id\":\"a010cbc3-0f61-4b9f-84ed-023e112c33c9\",\"name\":\"Science\"},\"section\":{\"id\":\"04d1f4d8-47e8-4f3d-a1f0-c233fb6eeec7\",\"name\":\"7-A\"}}}}}},\"result\":{},\"timestamp\":\"2019-07-23T05:53:34.239Z\"}",
              |   "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              |   "eventType":"student.mloLearnPage.content.completed",
              |   "tenantId":"21ce4d98-8286-4f7e-b122-03b98a5f3b2e",
              |   "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/STUDENT\",\"name\":\"42782fc0-ceaf-4792-b360-dede708a6999\"}},\"verb\":{\"id\":\"http://adlnet.gov/expapi/verbs/completed\",\"display\":{\"en-US\":\"completed\"}},\"object\":{\"id\":\"http://alefeducation.com/experience/541e9970-7f99-4302-9f6d-dfb25d53c936\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://alefeducation.com/xapi/activities/experience\",\"name\":{\"en-US\":\"The Big Idea\"}}},\"context\":{\"contextActivities\":{\"parent\":[{\"id\":\"http://alefeducation.com/learning-path/c6dd42c9-fbbf-46b6-87e7-3c388f629a95\"},{\"id\":\"http://alefeducation.com/lesson/e8995472-ea4f-4c40-a7e0-fc982cc4c3b6\",\"definition\":{\"name\":{\"en-US\":\"Types of Rocks\"}}}],\"grouping\":[{\"id\":\"http://alefeducation.com/curriculum/392027/grade/596550/subject/742980\"}]},\"extensions\":{\"http://alefeducation.com\":{\"attempt\":2,\"experienceId\":\"67f79f36-8bdb-41a2-a2d1-6e3178e59bbc\",\"isCompletionNode\":true,\"isFlexibleLesson\":true,\"learningSessionId\":\"6097a89f-3b71-4dd7-9a33-948363effb23\",\"timestampLocal\":\"Tue Jul 23 2019 09:56:44 GMT+0400 (Gulf Standard Time)\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36\",\"outsideOfSchool\":true,\"tenant\":{\"id\":\"21ce4d98-8286-4f7e-b122-03b98a5f3b2e\",\"school\":{\"academicCalendar\":{\"id\":\"d8308c71-0b33-4d4c-aa23-59f3c5e1cb2d\",\"currentPeriod\":{\"teachingPeriodId\":\"a56f6894-9a17-4c67-92bb-ec7b3445e9af\",\"teachingPeriodTitle\":\"Period 1\"}},\"id\":\"3b4775c8-2727-4e29-abfc-3d2cdfe92311\",\"name\":\"Turtle\",\"academicYearId\":\"b52342ad-8205-11e9-97db-0242ac110006\",\"grade\":{\"id\":\"ab0e0adc-2082-4ac7-9895-30bd7909ba2f\",\"name\":\"7\"},\"subject\":{\"id\":\"a010cbc3-0f61-4b9f-84ed-023e112c33c9\"},\"section\":{\"id\":\"04d1f4d8-47e8-4f3d-a1f0-c233fb6eeec7\",\"name\":\"7-A\"}}}}}},\"result\":{},\"timestamp\":\"2019-07-23T05:56:44.557Z\"}",
              |   "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              |   "eventType":"student.mloLearnPage.content.initialized",
              |   "tenantId":"21ce4d98-8286-4f7e-b122-03b98a5f3b2e",
              |   "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/STUDENT\",\"name\":\"42782fc0-ceaf-4792-b360-dede708a6999\"}},\"verb\":{\"id\":\"http://adlnet.gov/expapi/verbs/initialized\",\"display\":{\"en-US\":\"initialized\"}},\"object\":{\"id\":\"http://alefeducation.com/experience/2f8e9ba5-f154-424a-bd3f-06621c13fc0c\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://alefeducation.com/xapi/activities/experience\",\"name\":{\"en-US\":\"Check My Understanding 1\"}}},\"context\":{\"contextActivities\":{\"parent\":[{\"id\":\"http://alefeducation.com/learning-path/c6dd42c9-fbbf-46b6-87e7-3c388f629a95\"},{\"id\":\"http://alefeducation.com/lesson/e8995472-ea4f-4c40-a7e0-fc982cc4c3b6\",\"definition\":{\"name\":{\"en-US\":\"Types of Rocks\"}}}],\"grouping\":[{\"id\":\"http://alefeducation.com/curriculum/392027/grade/596550/subject/742980\"}]},\"extensions\":{\"http://alefeducation.com\":{\"attempt\":2,\"experienceId\":\"e134c827-fe4f-4e6f-9c2d-38a5594a1478\",\"learningSessionId\":\"6097a89f-3b71-4dd7-9a33-948363effb23\",\"timestampLocal\":\"Tue Jul 23 2019 09:56:52 GMT+0400 (Gulf Standard Time)\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.100 Safari/537.36\",\"outsideOfSchool\":true,\"tenant\":{\"id\":\"21ce4d98-8286-4f7e-b122-03b98a5f3b2e\",\"school\":{\"academicCalendar\":{\"id\":\"d8308c71-0b33-4d4c-aa23-59f3c5e1cb2d\",\"currentPeriod\":{\"teachingPeriodId\":\"a56f6894-9a17-4c67-92bb-ec7b3445e9af\",\"teachingPeriodTitle\":\"Period 1\"}},\"id\":\"3b4775c8-2727-4e29-abfc-3d2cdfe92311\",\"name\":\"Turtle\",\"academicYearId\":\"b52342ad-8205-11e9-97db-0242ac110006\",\"grade\":{\"id\":\"ab0e0adc-2082-4ac7-9895-30bd7909ba2f\",\"name\":\"7\"},\"subject\":{\"id\":\"a010cbc3-0f61-4b9f-84ed-023e112c33c9\"},\"section\":{\"id\":\"04d1f4d8-47e8-4f3d-a1f0-c233fb6eeec7\",\"name\":\"7-A\"}}}}}},\"result\":{},\"timestamp\":\"2019-07-23T05:56:52.379Z\"}",
              |   "loadtime":"APm21s0yAADlgyUA"
              |},
              |{
              |   "eventType":"student.platform.logout",
              |   "tenantId":"21ce4d98-8286-4f7e-b122-03b98a5f3b2e",
              |   "body":"{\"actor\":{\"objectType\":\"Agent\",\"account\":{\"homePage\":\"http://alefeducation.com/users/STUDENT\",\"name\":\"42782fc0-ceaf-4792-b360-dede708a6999\"}},\"verb\":{\"id\":\"https://brindlewaye.com/xAPITerms/verbs/logout/\",\"display\":{\"en-US\":\"Log Out\"}},\"object\":{\"id\":\"http://alefeducation.com/logout\",\"objectType\":\"Activity\",\"definition\":{\"type\":\"http://activitystrea.ms/schema/1.0/page\"}},\"context\":{\"contextActivities\":{\"parent\":[]},\"extensions\":{\"http://alefeducation.com\":{\"timestampLocal\":\"Tue Jul 23 2019 13:46:41 GMT+0400 (Gulf Standard Time)\",\"userAgent\":\"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/75.0.3770.142 Safari/537.36\",\"outsideOfSchool\":true,\"tenant\":{\"id\":\"21ce4d98-8286-4f7e-b122-03b98a5f3b2e\",\"school\":{\"academicCalendar\":{\"id\":\"d8308c71-0b33-4d4c-aa23-59f3c5e1cb2d\",\"currentPeriod\":{\"teachingPeriodId\":\"a56f6894-9a17-4c67-92bb-ec7b3445e9af\",\"teachingPeriodTitle\":\"Period 1\"}},\"id\":\"3b4775c8-2727-4e29-abfc-3d2cdfe92311\",\"name\":\"Turtle\",\"academicYearId\":\"b52342ad-8205-11e9-97db-0242ac110006\",\"grade\":{\"id\":\"ab0e0adc-2082-4ac7-9895-30bd7909ba2f\",\"name\":\"7\"},\"section\":{\"id\":\"04d1f4d8-47e8-4f3d-a1f0-c233fb6eeec7\",\"name\":\"7=A\"}}}}}},\"result\":{},\"timestamp\":\"2019-07-23T05:57:41.343Z\"}",
              |   "loadtime":"APm21s0yAADlgyUA"
              |}
              |]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val parquetSink = sinks.find(_.name == ParquetStudentXApiV2Source)
          assert(parquetSink.size == 1)
          val parquetDf = parquetSink.head.output
          assert(parquetDf.count == 6)
          assert(parquetSink.head.asInstanceOf[DataSink].options.get("partitionBy") === Some("eventdate,eventType"))
          val redshiftSink = sinks.find(_.name == RedshiftStudentActivitiesSink)
          assert(redshiftSink.size == 1)

          val redshiftDf = redshiftSink.head.output
          assert(redshiftDf.count == 6)

          val redshiftCompletedXApiDf =
            redshiftSink.head.output.filter("fsta_event_type = 'student.mloLearnPage.content.completed'")
          assert[Boolean](redshiftCompletedXApiDf, "fsta_is_completion_node", true)
          assert[Boolean](redshiftCompletedXApiDf, "fsta_is_flexible_lesson", true)

        }
      )
    }
  }
}
