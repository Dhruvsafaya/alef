package com.alefeducation.dimensions

import java.util.TimeZone
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.DataFrameEqualityUtils.{assertSmallDatasetEquality, createDfFromJson}
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataType, DateType, TimestampType}
import org.scalatest.matchers.must.Matchers

class CCLActivityDimensionSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer: CCLActivityDimension = new CCLActivityDimension(spark)
  }

  test("process Activity Metadata Created event") {
    val expParquetJson =
      """
        |[
        |{"academicYear":"2021","academicYearId":3,"activityType":"INSTRUCTIONAL_LESSON","activityUuid":"b01fadb3-1f60-4049-98f4-000000009957","boardId":392027,"code":"ac_001","createdAt":1625140715167,"description":"description_san","duration":25,"eventType":"ActivityMetadataCreatedEvent","gradeId":768780,"language":"EN_US","lessonId":9957,"lessonRoles":["REMEDIATION_LESSON"],"maxStars":0,"occurredOn":"2021-06-23 10:19:20.314","organisation":"shared","skillable":false,"status":"DRAFT","subjectId":186926,"themeIds":[13,14],"thumbnailContentType":"image/jpeg","thumbnailFileName":"ac_001_thumb.jpeg","thumbnailFileSize":6374,"thumbnailLocation":"0a/c6/bb/9957","thumbnailUpdatedAt":1625140715167,"title":"activity_001","userId":99999,"eventdate":"2021-06-23"}
        |]
      """.stripMargin
    val expRedshiftJson =
      """
        |[
        |{"lo_code":"ac_001","lo_max_stars":0,"lo_duration":25,"lo_organisation":"shared","lo_skillable":false,"lo_status":1,"lo_content_academic_year_id":3,"lo_curriculum_subject_id":186926,"lo_type":"INSTRUCTIONAL_LESSON","lo_language":"EN_US","lo_id":"b01fadb3-1f60-4049-98f4-000000009957","lo_curriculum_id":392027,"lo_content_academic_year":"2021","lo_action_status":1,"lo_user_id":99999,"lo_title":"activity_001","lo_curriculum_grade_id":768780,"lo_ccl_id":9957,"lo_created_time":"2021-06-23T10:19:20.314Z","lo_dw_created_time":"2021-07-12T07:03:22.918Z","lo_updated_time":null,"lo_dw_updated_time":null,"lo_deleted_time":null}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)
    val expDeltaJson =
      """
        |[
        |{"lo_code":"ac_001","lo_max_stars":0,"lo_duration":25,"lo_organisation":"shared","lo_description":"description_san","lo_skillable":false,"lo_status":1,"lo_content_academic_year_id":3,"lo_roles":["REMEDIATION_LESSON"],"lo_curriculum_subject_id":186926,"lo_type":"INSTRUCTIONAL_LESSON","lo_theme_ids":[13,14],"lo_language":"EN_US","lo_id":"b01fadb3-1f60-4049-98f4-000000009957","lo_curriculum_id":392027,"lo_content_academic_year":"2021","lo_action_status":1,"lo_user_id":99999,"lo_title":"activity_001","lo_curriculum_grade_id":768780,"lo_ccl_id":9957,"lo_created_time":"2021-06-23T10:19:20.314Z","lo_dw_created_time":"2021-07-12T07:06:55.557Z","lo_updated_time":null,"lo_dw_updated_time":null,"lo_deleted_time":null}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLActivityMetadataSource,
          value = """
                    |{
                    |   "eventType": "ActivityMetadataCreatedEvent",
                    |   "lessonId":9957,
                    |   "activityUuid":"b01fadb3-1f60-4049-98f4-000000009957",
                    |   "code":"ac_001",
                    |   "description":"description_san",
                    |   "title":"activity_001",
                    |   "activityType":"INSTRUCTIONAL_LESSON",
                    |   "boardId":392027,
                    |   "gradeId":768780,
                    |   "subjectId":186926,
                    |   "academicYearId":3,
                    |   "academicYear":"2021",
                    |   "thumbnailFileName":"ac_001_thumb.jpeg",
                    |   "thumbnailContentType":"image/jpeg",
                    |   "thumbnailFileSize":6374,
                    |   "thumbnailLocation":"0a/c6/bb/9957",
                    |   "thumbnailUpdatedAt":1625140715167,
                    |   "maxStars":0,
                    |   "lessonRoles":[
                    |      "REMEDIATION_LESSON"
                    |   ],
                    |   "themeIds":[
                    |      13, 14
                    |   ],
                    |   "duration":25,
                    |   "language":"EN_US",
                    |   "skillable":false,
                    |   "userId":99999,
                    |   "organisation":"shared",
                    |   "createdAt":1625140715167,
                    |   "status":"DRAFT",
                    |   "occurredOn":"2021-06-23 10:19:20.314"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquet = sinks.find(_.name === ParquetCCLActivityMetadataSink).get.output
          val redshift = sinks.find(_.name === RedshiftLearningObjectiveSink)
            .get.output
          val delta = sinks.find(_.name === DeltaCCLActivitySink)
            .get.output

          assertSmallDatasetEquality(spark, LearningObjectiveEntity, parquet, expParquetJson)
          assertSmallDatasetEquality(LearningObjectiveEntity, redshift, expRedshiftDf)
          assertSmallDatasetEquality(LearningObjectiveEntity, delta, expDeltaDf)
        }
      )
    }
  }

  test("process Activity Metadata Updated event") {
    val expParquetJson =
      """
        |[
        |{"academicYear":"2021","academicYearId":3,"activityType":"INSTRUCTIONAL_LESSON","activityUuid":"127a2a6e-6710-41a1-940c-000000009959","boardId":392027,"code":"fat_cat_001","description":"descfdsfdsf","duration":25,"eventType":"ActivityMetadataUpdatedEvent","gradeId":322135,"language":"EN_US","lessonId":9959,"lessonRoles":["CORE_LESSON"],"maxStars":0,"occurredOn":"2021-06-23 10:19:20.314","organisation":"shared","publishedBefore":true,"skillable":false,"status":"DRAFT","subjectId":186926,"themeIds":[13,14],"thumbnailContentType":"image/jpeg","thumbnailFileName":"fat_cat_001_thumb.jpeg","thumbnailFileSize":10156,"thumbnailLocation":"05/ec/04/9959","thumbnailUpdatedAt":1625491365948,"title":"fat_cat_want_eat","updatedAt":1625491365948,"userId":99999,"eventdate":"2021-06-23"}
        |]
      """.stripMargin
    val expRedshiftJson =
      """
        |[
        |{"lo_code":"fat_cat_001","lo_max_stars":0,"lo_duration":25,"lo_organisation":"shared","lo_skillable":false,"lo_status":1,"lo_content_academic_year_id":3,"lo_curriculum_subject_id":186926,"lo_type":"INSTRUCTIONAL_LESSON","lo_language":"EN_US","lo_id":"127a2a6e-6710-41a1-940c-000000009959","lo_curriculum_id":392027,"lo_content_academic_year":"2021","lo_action_status":1,"lo_user_id":99999,"lo_title":"fat_cat_want_eat","lo_curriculum_grade_id":322135,"lo_ccl_id":9959,"lo_created_time":"2021-06-23T10:19:20.314Z","lo_dw_created_time":"2021-07-12T07:34:57.319Z","lo_updated_time":null,"lo_dw_updated_time":null,"lo_deleted_time":null}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)
    val expDeltaJson =
      """
        |[
        |{"lo_code":"fat_cat_001","lo_max_stars":0,"lo_duration":25,"lo_organisation":"shared","lo_description":"descfdsfdsf","lo_skillable":false,"lo_status":1,"lo_content_academic_year_id":3,"lo_roles":["CORE_LESSON"],"lo_curriculum_subject_id":186926,"lo_type":"INSTRUCTIONAL_LESSON","lo_theme_ids":[13,14],"lo_language":"EN_US","lo_id":"127a2a6e-6710-41a1-940c-000000009959","lo_curriculum_id":392027,"lo_content_academic_year":"2021","lo_action_status":1,"lo_user_id":99999,"lo_title":"fat_cat_want_eat","lo_curriculum_grade_id":322135,"lo_ccl_id":9959,"lo_created_time":"2021-06-23T10:19:20.314Z","lo_dw_created_time":"2021-07-12T07:34:57.319Z","lo_updated_time":null,"lo_dw_updated_time":null,"lo_deleted_time":null}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLActivityMetadataSource,
          value = """
                    |{
                    |   "eventType":"ActivityMetadataUpdatedEvent",
                    |   "lessonId":9959,
                    |   "activityUuid":"127a2a6e-6710-41a1-940c-000000009959",
                    |   "code":"fat_cat_001",
                    |   "description":"descfdsfdsf",
                    |   "title":"fat_cat_want_eat",
                    |   "activityType":"INSTRUCTIONAL_LESSON",
                    |   "boardId":392027,
                    |   "gradeId":322135,
                    |   "subjectId":186926,
                    |   "academicYearId":3,
                    |   "academicYear":"2021",
                    |   "thumbnailFileName":"fat_cat_001_thumb.jpeg",
                    |   "thumbnailContentType":"image/jpeg",
                    |   "thumbnailFileSize":10156,
                    |   "thumbnailLocation":"05/ec/04/9959",
                    |   "thumbnailUpdatedAt":1625491365948,
                    |   "maxStars":0,
                    |   "lessonRoles":[
                    |      "CORE_LESSON"
                    |   ],
                    |   "themeIds":[
                    |       13, 14
                    |   ],
                    |   "duration":25,
                    |   "language":"EN_US",
                    |   "skillable":false,
                    |   "userId":99999,
                    |   "organisation":"shared",
                    |   "updatedAt":1625491365948,
                    |   "status":"DRAFT",
                    |   "publishedBefore":true,
                    |   "occurredOn":"2021-06-23 10:19:20.314"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquet = sinks.find(_.name === ParquetCCLActivityMetadataSink).get.output
          val redshift = sinks.find(_.name === RedshiftLearningObjectiveSink)
            .get.output
          val delta = sinks.find(_.name === DeltaCCLActivitySink)
            .get.output

          assertSmallDatasetEquality(spark, LearningObjectiveEntity, parquet, expParquetJson)
          assertSmallDatasetEquality(LearningObjectiveEntity, redshift, expRedshiftDf)
          assertSmallDatasetEquality(LearningObjectiveEntity, delta, expDeltaDf)
        }
      )
    }
  }

  test("process Activity Metadata Deleted event") {
    val expParquetJson =
      """
        |[
        |{"academicYear":"2021","activityUuid":"b01fadb3-1f60-4049-98f4-000000009957","code":"ac_001","eventType":"ActivityMetadataDeletedEvent","lessonId":9957,"occurredOn":"2021-06-23 10:19:20.314","eventdate":"2021-06-23"}
        |]
      """.stripMargin
    val expRedshiftJson =
      """
        |[
        |{"lo_ccl_id":9957,"lo_status":4,"lo_created_time":"2021-06-23T10:19:20.314Z","lo_dw_created_time":"2021-07-12T08:45:47.480Z","lo_deleted_time":"2021-06-23T10:19:20.314Z","lo_updated_time":null,"lo_dw_updated_time":null}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)
    val expDeltaJson =
      """
        |[
        |{"lo_ccl_id":9957,"lo_status":4,"lo_created_time":"2021-06-23T10:19:20.314Z","lo_dw_created_time":"2021-07-12T08:45:47.480Z","lo_deleted_time":"2021-06-23T10:19:20.314Z","lo_updated_time":null,"lo_dw_updated_time":null}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLActivityMetadataDeleteSource,
          value = """
                    |{
                    |   "eventType": "ActivityMetadataDeletedEvent",
                    |   "lessonId":9957,
                    |   "activityUuid":"b01fadb3-1f60-4049-98f4-000000009957",
                    |   "code":"ac_001",
                    |   "academicYear":"2021",
                    |   "occurredOn":"2021-06-23 10:19:20.314"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquet = sinks.find(_.name === ParquetCCLActivityMetadataDeleteSink).get.output
          val redshift = sinks.find(_.name === RedshiftLearningObjectiveSink)
            .get.output
          val delta = sinks.find(_.name === DeltaCCLActivitySink)
            .get.output

          assertSmallDatasetEquality(spark, LearningObjectiveEntity, parquet, expParquetJson)
          assertSmallDatasetEquality(LearningObjectiveEntity, redshift, expRedshiftDf)
          assertSmallDatasetEquality(LearningObjectiveEntity, delta, expDeltaDf)
        }
      )
    }
  }

  test("process Activity Workflow Status Changed event") {
    val expParquetJson =
      """
        |[
        |{"academicYear":"2019","activityUuid":"8dfd2ba3-36a2-4d0d-adf8-000000009608","code":"lesson_step_uuid_new","eventType":"ActivityWorkflowStatusChangedEvent","lessonId":9608,"occurredOn":"2021-06-23 10:19:20.314","publishedBefore":true,"status":"DRAFT","eventdate":"2021-06-23"}
        |]
      """.stripMargin
    val expRedshiftJson =
      """
        |[
        |{"lo_ccl_id":9608,"lo_action_status":1,"lo_created_time":"2021-06-23T10:19:20.314Z","lo_dw_created_time":"2021-07-12T09:27:39.945Z","lo_updated_time":null,"lo_deleted_time":null,"lo_dw_updated_time":null}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)
    val expDeltaJson =
      """
        |[
        |{"lo_ccl_id":9608,"lo_action_status":1,"lo_created_time":"2021-06-23T10:19:20.314Z","lo_dw_created_time":"2021-07-12T09:27:39.945Z","lo_updated_time":null,"lo_deleted_time":null,"lo_dw_updated_time":null}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLActivityWorkflowStatusChangedSource,
          value = """
                    |{
                    |   "eventType": "ActivityWorkflowStatusChangedEvent",
                    |   "lessonId":9608,
                    |   "activityUuid":"8dfd2ba3-36a2-4d0d-adf8-000000009608",
                    |   "status":"DRAFT",
                    |   "publishedBefore":true,
                    |   "academicYear":"2019",
                    |   "code":"lesson_step_uuid_new",
                    |   "occurredOn":"2021-06-23 10:19:20.314"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquet = sinks.find(_.name === ParquetCCLActivityWorkflowStatusChangedSink).get.output
          val redshift = sinks.find(_.name === RedshiftLearningObjectiveSink)
            .get.output
          val delta = sinks.find(_.name === DeltaCCLActivitySink)
            .get.output

          assertSmallDatasetEquality(spark, LearningObjectiveEntity, parquet, expParquetJson)
          assertSmallDatasetEquality(LearningObjectiveEntity, redshift, expRedshiftDf)
          assertSmallDatasetEquality(LearningObjectiveEntity, delta, expDeltaDf)
        }
      )
    }
  }

  test("process Activity Metadata Published event") {
    val expParquetJson =
      """
        |[
        |{"academicYear":"2019","academicYearId":1,"activityTemplateUuid":"44548889-69ae-4170-8edb-82f7a22a50ff","activityType":"INSTRUCTIONAL_LESSON","activityUuid":"b237cd53-8888-4485-9485-000000009935","boardId":392027,"code":"test_game_service","components":[{"activityComponentType":"CONTENT","activityTemplateComponentId":"3e06dddb-9b22-464e-b5cf-bb5ec09c110d","cclContentId":40950,"description":"cont","location":"https://ccl-panthers.nprd.alefed.com/data/ccl/content/f1/29/1f/d5/40950/AR8_MLO_174_THUMB.png","mediaType":"STATIC","stepUuid":"60996688-7237-4222-a301-e52820892796","title":"cont"},{"activityComponentType":"ASSESSMENT","activityTemplateComponentId":"b032975f-bbf2-4627-9ffa-1ab2d15fa610","assessmentRules":[{"difficultyLevel":"ANY","id":"25b05d50-fcb2-42e6-a8bf-21eab1955743","poolId":"60ae8fb39c9a350001dc64b2","poolName":"MA5_REV_006_AR","questions":2,"resourceType":"ANY"}],"description":"assessment","location":"https://shared.alefed.com/alef-assessment?rules=5cfd0066f7ecd20001c6c31d;ANY;ANY;1&language=EN_US","mediaType":"INTERACTIVE","stepUuid":"c7e3b773-bc73-4e05-a09c-c9bbf8c4535f","title":"assessment"},{"activityComponentType":"ASSIGNMENT","activityTemplateComponentId":"c0a42b8a-d0ae-49c3-9ca1-3d43a762e132","assessmentRules":[],"assignmentId":"323c52a4-bd84-47e2-a5f4-dadf92415ed1","description":"Aefds","location":"https://shared.alefed.com/alef-assignments?assignmentId=8d57bf06-cc34-4dc6-a71b-312bbf7d14ff","mediaType":"STATIC","stepUuid":"ee6152e2-95ee-47b4-a2e0-987b48cda411","title":"Aefds"}],"description":"test_game_service","duration":0,"eventType":"ActivityPublishedEvent","gradeId":333938,"language":"EN_US","lessonId":9935,"lessonRoles":["CORE_LESSON"],"maxStars":2,"occurredOn":"2021-06-23 10:19:20.314","publishedBefore":true,"publishedDate":"2021-06-24T06:53:58.062565","publisherId":99999,"skillable":false,"subjectId":1,"themeIds":[13,14],"thumbnailContentType":"image/jpeg","thumbnailFileName":"test_game_service_thumb.jpeg","thumbnailFileSize":63449,"thumbnailLocation":"5d/29/33/9935","thumbnailUpdatedAt":1624516551838,"title":"test_game_service","eventdate":"2021-06-23"}
        |]
      """.stripMargin
    val expRedshiftJson =
      """
        |[
        |{"lo_template_uuid":"44548889-69ae-4170-8edb-82f7a22a50ff","lo_code":"test_game_service","lo_publisher_id":99999,"lo_max_stars":2,"lo_duration":0,"lo_skillable":false,"lo_status":1,"lo_content_academic_year_id":1,"lo_curriculum_subject_id":1,"lo_type":"INSTRUCTIONAL_LESSON","lo_language":"EN_US","lo_id":"b237cd53-8888-4485-9485-000000009935","lo_published_date":"2021-06-24","lo_curriculum_id":392027,"lo_content_academic_year":"2019","lo_action_status":5,"lo_title":"test_game_service","lo_curriculum_grade_id":333938,"lo_ccl_id":9935,"lo_created_time":"2021-06-23T10:19:20.314Z","lo_dw_created_time":"2021-07-12T10:09:32.583Z","lo_updated_time":null,"lo_dw_updated_time":null,"lo_deleted_time":null}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)
    val expDeltaJson =
      """
        |[
        |{"lo_template_uuid":"44548889-69ae-4170-8edb-82f7a22a50ff","lo_code":"test_game_service","lo_publisher_id":99999,"lo_max_stars":2,"lo_duration":0,"lo_description":"test_game_service","lo_skillable":false,"lo_status":1,"lo_content_academic_year_id":1,"lo_roles":["CORE_LESSON"],"lo_curriculum_subject_id":1,"lo_type":"INSTRUCTIONAL_LESSON","lo_theme_ids":[13,14],"lo_language":"EN_US","lo_id":"b237cd53-8888-4485-9485-000000009935","lo_published_date":"2021-06-24","lo_curriculum_id":392027,"lo_content_academic_year":"2019","lo_action_status":5,"lo_title":"test_game_service","lo_curriculum_grade_id":333938,"lo_ccl_id":9935,"lo_created_time":"2021-06-23T10:19:20.314Z","lo_dw_created_time":"2021-07-12T10:09:32.583Z","lo_updated_time":null,"lo_dw_updated_time":null,"lo_deleted_time":null}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    val expRedshiftComponentsJson =
      """
        |[
        |{"step_instance_attach_status":1,"step_instance_resource_type":"ANY","step_instance_title":"assessment","step_instance_media_type":"INTERACTIVE","step_instance_id":"25b05d50-fcb2-42e6-a8bf-21eab1955743","step_instance_questions":2,"step_instance_template_uuid":"b032975f-bbf2-4627-9ffa-1ab2d15fa610","step_instance_type":5,"step_instance_pool_name":"MA5_REV_006_AR","step_instance_lo_id":"b237cd53-8888-4485-9485-000000009935","step_instance_status":1,"step_instance_step_uuid":"c7e3b773-bc73-4e05-a09c-c9bbf8c4535f","step_instance_pool_id":"60ae8fb39c9a350001dc64b2","step_instance_difficulty_level":"ANY","step_instance_lo_ccl_id":9935,"step_instance_created_time":"2021-06-23T10:19:20.314Z","step_instance_dw_created_time":"2021-07-26T09:19:36.733Z","step_instance_updated_time":null,"step_instance_dw_updated_time":null},
        |{"step_instance_attach_status":1,"step_instance_title":"Aefds","step_instance_media_type":"STATIC","step_instance_id":"323c52a4-bd84-47e2-a5f4-dadf92415ed1","step_instance_template_uuid":"c0a42b8a-d0ae-49c3-9ca1-3d43a762e132","step_instance_type":4,"step_instance_lo_id":"b237cd53-8888-4485-9485-000000009935","step_instance_status":1,"step_instance_step_uuid":"ee6152e2-95ee-47b4-a2e0-987b48cda411","step_instance_lo_ccl_id":9935,"step_instance_created_time":"2021-06-23T10:19:20.314Z","step_instance_dw_created_time":"2021-07-26T09:19:36.733Z","step_instance_updated_time":null},
        |{"step_instance_attach_status":1,"step_instance_title":"cont","step_instance_media_type":"STATIC","step_instance_id":"40950","step_instance_template_uuid":"3e06dddb-9b22-464e-b5cf-bb5ec09c110d","step_instance_type":1,"step_instance_lo_id":"b237cd53-8888-4485-9485-000000009935","step_instance_status":1,"step_instance_step_uuid":"60996688-7237-4222-a301-e52820892796","step_instance_lo_ccl_id":9935,"step_instance_created_time":"2021-06-23T10:19:20.314Z","step_instance_dw_created_time":"2021-07-26T09:19:36.733Z","step_instance_updated_time":null}
        |]
      """.stripMargin
    val expRedshiftComponentsDf = createDfFromJsonWithTimeCols(spark, expRedshiftComponentsJson)

    val expDeltaComponentsJson =
      """
        |[
        |{"step_instance_attach_status":1,"step_instance_resource_type":"ANY","step_instance_title":"assessment","step_instance_location":"https://shared.alefed.com/alef-assessment?rules=5cfd0066f7ecd20001c6c31d;ANY;ANY;1&language=EN_US","step_instance_media_type":"INTERACTIVE","step_instance_id":"25b05d50-fcb2-42e6-a8bf-21eab1955743","step_instance_questions":2,"step_instance_template_uuid":"b032975f-bbf2-4627-9ffa-1ab2d15fa610","step_instance_type":5,"step_instance_pool_name":"MA5_REV_006_AR","step_instance_lo_id":"b237cd53-8888-4485-9485-000000009935","step_instance_status":1,"step_instance_step_uuid":"c7e3b773-bc73-4e05-a09c-c9bbf8c4535f","step_instance_pool_id":"60ae8fb39c9a350001dc64b2","step_instance_description":"assessment","step_instance_difficulty_level":"ANY","step_instance_lo_ccl_id":9935,"step_instance_created_time":"2021-06-23T10:19:20.314Z","step_instance_dw_created_time":"2021-07-26T09:19:36.733Z","step_instance_updated_time":null,"step_instance_dw_updated_time":null},
        |{"step_instance_attach_status":1,"step_instance_title":"Aefds","step_instance_location":"https://shared.alefed.com/alef-assignments?assignmentId=8d57bf06-cc34-4dc6-a71b-312bbf7d14ff","step_instance_media_type":"STATIC","step_instance_id":"323c52a4-bd84-47e2-a5f4-dadf92415ed1","step_instance_template_uuid":"c0a42b8a-d0ae-49c3-9ca1-3d43a762e132","step_instance_type":4,"step_instance_lo_id":"b237cd53-8888-4485-9485-000000009935","step_instance_status":1,"step_instance_step_uuid":"ee6152e2-95ee-47b4-a2e0-987b48cda411","step_instance_description":"Aefds","step_instance_lo_ccl_id":9935,"step_instance_created_time":"2021-06-23T10:19:20.314Z","step_instance_dw_created_time":"2021-07-26T09:19:36.733Z","step_instance_updated_time":null},
        |{"step_instance_attach_status":1,"step_instance_title":"cont","step_instance_location":"https://ccl-panthers.nprd.alefed.com/data/ccl/content/f1/29/1f/d5/40950/AR8_MLO_174_THUMB.png","step_instance_media_type":"STATIC","step_instance_id":"40950","step_instance_template_uuid":"3e06dddb-9b22-464e-b5cf-bb5ec09c110d","step_instance_type":1,"step_instance_lo_id":"b237cd53-8888-4485-9485-000000009935","step_instance_status":1,"step_instance_step_uuid":"60996688-7237-4222-a301-e52820892796","step_instance_description":"cont","step_instance_lo_ccl_id":9935,"step_instance_created_time":"2021-06-23T10:19:20.314Z","step_instance_dw_created_time":"2021-07-26T09:19:36.733Z","step_instance_updated_time":null}
        |]
      """.stripMargin
    val expDeltaComponentsDf = createDfFromJsonWithTimeCols(spark, expDeltaComponentsJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLActivityMetadataSource,
          value = """
                    |{
                    |   "eventType": "ActivityPublishedEvent",
                    |   "lessonId":9935,
                    |   "activityUuid":"b237cd53-8888-4485-9485-000000009935",
                    |   "activityTemplateUuid":"44548889-69ae-4170-8edb-82f7a22a50ff",
                    |   "code":"test_game_service",
                    |   "boardId":392027,
                    |   "gradeId":333938,
                    |   "subjectId":1,
                    |   "academicYearId":1,
                    |   "academicYear":"2019",
                    |   "title":"test_game_service",
                    |   "description":"test_game_service",
                    |   "thumbnailFileName":"test_game_service_thumb.jpeg",
                    |   "thumbnailContentType":"image/jpeg",
                    |   "thumbnailFileSize":63449,
                    |   "thumbnailLocation":"5d/29/33/9935",
                    |   "thumbnailUpdatedAt":1624516551838,
                    |   "maxStars":2,
                    |   "lessonRoles":[
                    |       "CORE_LESSON"
                    |   ],
                    |   "activityType":"INSTRUCTIONAL_LESSON",
                    |   "themeIds":[
                    |       13, 14
                    |   ],
                    |   "duration":0,
                    |   "language":"EN_US",
                    |   "skillable":false,
                    |   "publisherId":99999,
                    |   "publishedDate":"2021-06-24T06:53:58.062565",
                    |   "publishedBefore":true,
                    |   "components":[
                    |      {
                    |         "stepUuid":"60996688-7237-4222-a301-e52820892796",
                    |         "activityTemplateComponentId":"3e06dddb-9b22-464e-b5cf-bb5ec09c110d",
                    |         "title":"cont",
                    |         "description":"cont",
                    |         "location":"https://ccl-panthers.nprd.alefed.com/data/ccl/content/f1/29/1f/d5/40950/AR8_MLO_174_THUMB.png",
                    |         "activityComponentType":"CONTENT",
                    |         "mediaType":"STATIC",
                    |         "cclContentId":40950,
                    |         "assignmentId": null,
                    |         "assessmentRules": null
                    |      },
                    |      {
                    |         "stepUuid":"c7e3b773-bc73-4e05-a09c-c9bbf8c4535f",
                    |         "activityTemplateComponentId":"b032975f-bbf2-4627-9ffa-1ab2d15fa610",
                    |         "title":"assessment",
                    |         "description":"assessment",
                    |         "location":"https://shared.alefed.com/alef-assessment?rules=5cfd0066f7ecd20001c6c31d;ANY;ANY;1&language=EN_US",
                    |         "activityComponentType":"ASSESSMENT",
                    |         "mediaType":"INTERACTIVE",
                    |         "cclContentId":null,
                    |         "assignmentId":null,
                    |         "assessmentRules":[
                    |             {
                    |                "id":"25b05d50-fcb2-42e6-a8bf-21eab1955743",
                    |                "poolId":"60ae8fb39c9a350001dc64b2",
                    |                "poolName":"MA5_REV_006_AR",
                    |                "difficultyLevel":"ANY",
                    |                "resourceType":"ANY",
                    |                "questions":2
                    |             }
                    |        ]
                    |      },
                    |      {
                    |         "stepUuid":"ee6152e2-95ee-47b4-a2e0-987b48cda411",
                    |         "activityTemplateComponentId":"c0a42b8a-d0ae-49c3-9ca1-3d43a762e132",
                    |         "title":"Aefds",
                    |         "description":"Aefds",
                    |         "location":"https://shared.alefed.com/alef-assignments?assignmentId=8d57bf06-cc34-4dc6-a71b-312bbf7d14ff",
                    |         "activityComponentType":"ASSIGNMENT",
                    |         "mediaType":"STATIC",
                    |         "cclContentId":null,
                    |         "assignmentId":"323c52a4-bd84-47e2-a5f4-dadf92415ed1",
                    |         "assessmentRules":[]
                    |      }
                    |   ],
                    |   "occurredOn":"2021-06-23 10:19:20.314"
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquet = sinks.find(_.name === ParquetCCLActivityMetadataSink).get.output
          val redshift = sinks.find(_.name === RedshiftLearningObjectiveSink)
            .get.output
          val delta = sinks.find(_.name === DeltaCCLActivitySink)
            .get.output
          val redshiftComponents = sinks.find(_.name === RedshiftLessonStepInstanceSink)
            .get.output
          val deltaComponents = sinks.find(_.name === DeltaCCLLessonContentStepInstanceSink)
            .get.output

          assertSmallDatasetEquality(spark, LearningObjectiveEntity, parquet, expParquetJson)
          assertSmallDatasetEquality(LearningObjectiveEntity, redshift, expRedshiftDf)
          assertSmallDatasetEquality(LearningObjectiveEntity, delta, expDeltaDf)

          assertSmallDatasetEquality(LearningObjectiveStepInstanceEntity, redshiftComponents, expRedshiftComponentsDf)
          assertSmallDatasetEquality(LearningObjectiveStepInstanceEntity, deltaComponents, expDeltaComponentsDf)
        }
      )
    }
  }

  def createDfFromJsonWithTimeCols(spark: SparkSession, json: String): DataFrame = {
    createDfFromJson(spark, json)
      .transform(castToTimestampIfExists("lo_created_time", TimestampType))
      .transform(castToTimestampIfExists("lo_updated_time", TimestampType))
      .transform(castToTimestampIfExists("lo_deleted_time", TimestampType))
      .transform(castToTimestampIfExists("lo_published_date", DateType))
      .transform(castToTimestampIfExists("step_instance_created_time", TimestampType))
      .transform(castToTimestampIfExists("step_instance_updated_time", TimestampType))
  }

  private def castToTimestampIfExists(name: String, tp: DataType)(df: DataFrame): DataFrame =
    if (df.columns.contains(name)) {
      df.withColumn(name, col(name).cast(tp))
    } else df

}
