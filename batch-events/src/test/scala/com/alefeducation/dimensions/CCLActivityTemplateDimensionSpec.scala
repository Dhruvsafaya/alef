package com.alefeducation.dimensions

import java.util.TimeZone

import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.DataFrameEqualityUtils.{assertSmallDatasetEquality, createDfFromJson}
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.matchers.must.Matchers

class CCLActivityTemplateDimensionSpec extends SparkSuite with Matchers {

  trait Setup {
    implicit val transformer: CCLActivityTemplateDimension = new CCLActivityTemplateDimension(spark)
  }

  test("save Activity Template event") {
    val expParquetJson =
      """
        |[
        |{"activityType":"INSTRUCTIONAL_LESSON","dateCreated":"2020-08-13T08:15:25.529","description":"given description","eventType":"ActivityTemplatePublishedEvent","mainComponents":[{"abbreviation":"abbr1","alwaysEnabled":false,"assessmentsAttempts":2,"completionNode":true,"exitTicket":false,"icon":"bigIdea-1","id":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","maxRepeat":2,"name":"component 1","order":1,"passingScore":80,"performanceConditions":[{"max":100,"min":0,"releaseComponent":"80sn3d57-6e58-4554-ad71-f82e2cflsj06"}],"questionAttempts":{"attempts":2,"hints":false},"releaseCondition":"","releaseConditions":null,"type":"ASSESSMENT"}],"name":"name 1","occurredOn":1597306525992,"publishedDate":"2020-08-13T08:15:25.529","publisherId":99999,"publisherName":"john doe","sideComponents":[{"abbreviation":"abbr1","alwaysEnabled":true,"completionNode":false,"exitTicket":false,"icon":"bigIdea-1","id":"20sn3d57-6e58-4554-ad71-f82e2ckasj96","name":"component 1","order":0,"performanceConditions":[],"type":"KEY_TERM","passingScore":null,"assessmentsAttempts":null,"questionAttempts":null,"releaseCondition": null,"releaseConditions": null,"maxRepeat":null}],"status":"PUBLISHED","supportingComponents":[{"abbreviation":"abbr1","alwaysEnabled":false,"assessmentsAttempts":2,"completionNode":true,"exitTicket":false,"icon":"bigIdea-1","id":"80sn3d57-6e58-4554-ad71-f82e2cflsj06","maxRepeat":2,"name":"component 1","order":1,"passingScore":80,"performanceConditions":[],"questionAttempts":{"attempts":2,"hints":false},"releaseCondition":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","releaseConditions":["80sn3d57-6e58-4554-ad71-f82e2cflsj03"],"type":"CONTENT"}],"templateUuid":"830a4d57-6e58-4554-ad71-f82e2cf9c6b3","eventdate":"2020-08-13"}
        |]
      """.stripMargin
    val expRedshiftJson =
      """
        |[
        |{"at_publisher_id":99999,"at_release_component":"80sn3d57-6e58-4554-ad71-f82e2cflsj06","at_section_type":1,"at_component_name":"component 1","at_question_attempts":2,"at_exit_ticket":false,"at_publisher_name":"john doe","at_name":"name 1","at_always_enabled":false,"at_description":"given description","at_icon":"bigIdea-1","at_min":0,"at_completion_node":true,"at_question_attempts_hints":false,"at_max_repeat":2,"at_assessments_attempts":2,"at_activity_type":"INSTRUCTIONAL_LESSON","at_component_uuid":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","at_max":100,"at_order":1,"at_uuid":"830a4d57-6e58-4554-ad71-f82e2cf9c6b3","at_published_date":"2020-08-13T08:15:25.529Z","at_release_condition":"","at_component_type":5,"at_abbreviation":"abbr1","at_status":1,"at_passing_score":80,"at_created_time":"2020-08-13T08:15:25.529Z","at_dw_created_time":"2021-06-23T09:57:57.320Z","at_updated_time":null,"at_dw_updated_time":null},
        |{"at_publisher_id":99999,"at_section_type":2,"at_component_name":"component 1","at_question_attempts":2,"at_exit_ticket":false,"at_publisher_name":"john doe","at_name":"name 1","at_always_enabled":false,"at_description":"given description","at_icon":"bigIdea-1","at_completion_node":true,"at_question_attempts_hints":false,"at_max_repeat":2,"at_assessments_attempts":2,"at_activity_type":"INSTRUCTIONAL_LESSON","at_component_uuid":"80sn3d57-6e58-4554-ad71-f82e2cflsj06","at_order":1,"at_uuid":"830a4d57-6e58-4554-ad71-f82e2cf9c6b3","at_published_date":"2020-08-13T08:15:25.529Z","at_release_condition":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","at_component_type":1,"at_abbreviation":"abbr1","at_status":1,"at_passing_score":80,"at_created_time":"2020-08-13T08:15:25.529Z","at_dw_created_time":"2021-06-23T09:57:57.320Z","at_updated_time":null,"at_dw_updated_time":null},
        |{"at_publisher_id":99999,"at_section_type":3,"at_component_name":"component 1","at_exit_ticket":false,"at_publisher_name":"john doe","at_name":"name 1","at_always_enabled":true,"at_description":"given description","at_icon":"bigIdea-1","at_completion_node":false,"at_activity_type":"INSTRUCTIONAL_LESSON","at_component_uuid":"20sn3d57-6e58-4554-ad71-f82e2ckasj96","at_order":0,"at_uuid":"830a4d57-6e58-4554-ad71-f82e2cf9c6b3","at_published_date":"2020-08-13T08:15:25.529Z","at_component_type":6,"at_abbreviation":"abbr1","at_status":1,"at_created_time":"2020-08-13T08:15:25.529Z","at_dw_created_time":"2021-06-23T09:57:57.320Z","at_updated_time":null,"at_dw_updated_time":null,"at_question_attempts":null,"at_question_attempts_hints":null,"at_passing_score":null,"at_max_repeat":null,"at_assessments_attempts":null,"at_release_condition":null}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)
    val expDeltaJson =
      """
        |[
        |{"at_publisher_id":99999,"at_name":"name 1","at_publisher_name":"john doe","at_description":"given description","supportingComponents":[{"abbreviation":"abbr1","alwaysEnabled":false,"assessmentsAttempts":2,"completionNode":true,"exitTicket":false,"icon":"bigIdea-1","id":"80sn3d57-6e58-4554-ad71-f82e2cflsj06","maxRepeat":2,"name":"component 1","order":1,"passingScore":80,"performanceConditions":[],"questionAttempts":{"attempts":2,"hints":false},"releaseCondition":"80sn3d57-6e58-4554-ad71-f82e2cflsj03", "releaseConditions":["80sn3d57-6e58-4554-ad71-f82e2cflsj03"],"type":"CONTENT"}],"mainComponents":[{"abbreviation":"abbr1","alwaysEnabled":false,"assessmentsAttempts":2,"completionNode":true,"exitTicket":false,"icon":"bigIdea-1","id":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","maxRepeat":2,"name":"component 1","order":1,"passingScore":80,"performanceConditions":[{"max":100,"min":0,"releaseComponent":"80sn3d57-6e58-4554-ad71-f82e2cflsj06"}],"questionAttempts":{"attempts":2,"hints":false},"releaseCondition":"","releaseConditions":null,"type":"ASSESSMENT"}],"at_activity_type":"INSTRUCTIONAL_LESSON","at_id":"830a4d57-6e58-4554-ad71-f82e2cf9c6b3","at_published_date":"2020-08-13 08:15:25.529","sideComponents":[{"abbreviation":"abbr1","alwaysEnabled":true,"completionNode":false,"exitTicket":false,"icon":"bigIdea-1","id":"20sn3d57-6e58-4554-ad71-f82e2ckasj96","name":"component 1","order":0,"performanceConditions":[],"type":"KEY_TERM","passingScore":null,"assessmentsAttempts":null,"questionAttempts":null,"releaseCondition":null,"releaseConditions":null,"maxRepeat":null}],"at_status":1,"at_created_time":"2020-08-13 08:15:25.529Z","at_dw_created_time":"2021-06-23 10:19:48.794Z","at_dw_updated_time":null,"at_updated_time":null}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLActivityTemplateSource,
          value = """
                    |{
                    |   "eventType": "ActivityTemplatePublishedEvent",
                    |	  "templateUuid": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
                    |	  "name": "name 1",
                    |	  "description": "given description",
                    |	  "status": "PUBLISHED",
                    |	  "activityType": "INSTRUCTIONAL_LESSON",
                    |	  "dateCreated": "2020-08-13T08:15:25.529",
                    |	  "publishedDate": "2020-08-13T08:15:25.529",
                    |	  "publisherId": 99999,
                    |	  "publisherName": "john doe",
                    |	  "mainComponents": [
                    |	    {
                    |	      "id": "80sn3d57-6e58-4554-ad71-f82e2cflsj03",
                    |	      "order": 1,
                    |	      "name": "component 1",
                    |	      "type": "ASSESSMENT",
                    |	      "abbreviation": "abbr1",
                    |	      "icon": "bigIdea-1",
                    |	      "maxRepeat": 2,
                    |	      "exitTicket": false,
                    |	      "completionNode": true,
                    |	      "alwaysEnabled": false,
                    |	      "passingScore": 80,
                    |	      "assessmentsAttempts": 2,
                    |	      "questionAttempts": {
                    |           "hints": false,
                    |           "attempts": 2
                    |       },
                    |	      "releaseCondition": "",
                    |       "releaseConditions": null,
                    |	      "performanceConditions": [
                    |	        {"releaseComponent": "80sn3d57-6e58-4554-ad71-f82e2cflsj06", "min": 0, "max": 100}
                    |	      ]
                    |	    }
                    |	  ],
                    |	  "supportingComponents": [
                    |	    {
                    |	      "id": "80sn3d57-6e58-4554-ad71-f82e2cflsj06",
                    |	      "order": 1,
                    |	      "name": "component 1",
                    |	      "type": "CONTENT",
                    |	      "abbreviation": "abbr1",
                    |	      "icon": "bigIdea-1",
                    |	      "maxRepeat": 2,
                    |	      "exitTicket": false,
                    |	      "completionNode": true,
                    |	      "alwaysEnabled": false,
                    |	      "passingScore": 80,
                    |	      "assessmentsAttempts": 2,
                    |	      "questionAttempts": {
                    |           "hints": false,
                    |           "attempts": 2
                    |       },
                    |	      "releaseCondition": "80sn3d57-6e58-4554-ad71-f82e2cflsj03",
                    |       "releaseConditions": ["80sn3d57-6e58-4554-ad71-f82e2cflsj03"],
                    |	      "performanceConditions": []
                    |	    }
                    |	  ],
                    |	  "sideComponents": [
                    |	    {
                    |	      "id": "20sn3d57-6e58-4554-ad71-f82e2ckasj96",
                    |	      "order": 0,
                    |	      "name": "component 1",
                    |	      "type": "KEY_TERM",
                    |	      "abbreviation": "abbr1",
                    |	      "icon": "bigIdea-1",
                    |	      "maxRepeat": null,
                    |	      "exitTicket": false,
                    |	      "completionNode": false,
                    |	      "alwaysEnabled": true,
                    |	      "passingScore": null,
                    |	      "assessmentsAttempts": null,
                    |	      "questionAttempts": null,
                    |	      "releaseCondition": null,
                    |       "releaseConditions": null,
                    |	      "performanceConditions": []
                    |	    }
                    |	  ],
                    |	  "occurredOn": 1597306525992
                    |}
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquet = sinks.find(_.name === ParquetCCLActivityTemplateSink).get.output
          val redshift = sinks.find(_.name === RedshiftCCLActivityTemplateSink)
            .get.output
          val delta = sinks.find(_.name === DeltaCCLActivityTemplateSink)
            .get.output

          assertSmallDatasetEquality(spark, ActivityTemplateEntity, parquet, expParquetJson)
          assertSmallDatasetEquality(ActivityTemplateEntity, redshift, expRedshiftDf)
          assertSmallDatasetEquality(ActivityTemplateEntity, delta, expDeltaDf)
        }
      )
    }
  }

  test("update Activity Template event") {
    val expParquetJson =
      """
        |[
        |{"activityType":"INSTRUCTIONAL_LESSON","dateCreated":"2020-08-13T08:15:25.529","description":"given description","eventType":"ActivityTemplatePublishedEvent","mainComponents":[{"abbreviation":"abbr1","alwaysEnabled":false,"assessmentsAttempts":2,"completionNode":true,"exitTicket":false,"icon":"bigIdea-1","id":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","maxRepeat":2,"name":"component 1","order":1,"passingScore":80,"performanceConditions":[{"max":100,"min":0,"releaseComponent":"80sn3d57-6e58-4554-ad71-f82e2cflsj06"}],"questionAttempts":{"attempts":2,"hints":false},"releaseCondition":"","type":"ASSESSMENT"}],"name":"name 1","occurredOn":1597306525992,"publishedDate":"2020-08-13T08:15:25.529","publisherId":99999,"publisherName":"john doe","sideComponents":[{"abbreviation":"abbr1","alwaysEnabled":true,"completionNode":false,"exitTicket":false,"icon":"bigIdea-1","id":"20sn3d57-6e58-4554-ad71-f82e2ckasj96","name":"component 1","order":0,"performanceConditions":[],"type":"KEY_TERM","passingScore":null,"assessmentsAttempts":null,"questionAttempts":null,"releaseCondition":null,"maxRepeat":null}],"status":"PUBLISHED","supportingComponents":[{"abbreviation":"abbr1","alwaysEnabled":false,"assessmentsAttempts":2,"completionNode":true,"exitTicket":false,"icon":"bigIdea-1","id":"80sn3d57-6e58-4554-ad71-f82e2cflsj06","maxRepeat":2,"name":"component 1","order":1,"passingScore":80,"performanceConditions":[],"questionAttempts":{"attempts":2,"hints":false},"releaseCondition":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","type":"CONTENT"}],"templateUuid":"830a4d57-6e58-4554-ad71-f82e2cf9c6b3","eventdate":"2020-08-13"},
        |{"activityType":"INSTRUCTIONAL_LESSON","dateCreated":"2020-08-13T08:15:27.529","description":"given description UPDATED","eventType":"ActivityTemplatePublishedEvent","mainComponents":[{"abbreviation":"abbr1","alwaysEnabled":false,"assessmentsAttempts":3,"completionNode":true,"exitTicket":false,"icon":"bigIdea-1","id":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","maxRepeat":2,"name":"component 1 UPDATED","order":1,"passingScore":80,"performanceConditions":[{"max":90,"min":0,"releaseComponent":"80sn3d57-6e58-4554-ad71-f82e2cflsj06"}],"questionAttempts":{"attempts":3,"hints":false},"releaseCondition":"","type":"ASSESSMENT"}],"name":"name 1 UPDATED","occurredOn":1597306526992,"publishedDate":"2020-08-13T08:15:27.529","publisherId":99999,"publisherName":"john doe","sideComponents":[{"abbreviation":"abbr1","alwaysEnabled":true,"completionNode":false,"exitTicket":false,"icon":"bigIdea-1","id":"20sn3d57-6e58-4554-ad71-f82e2ckasj96","name":"component 1 UPDATED","order":0,"performanceConditions":[],"type":"KEY_TERM","passingScore":null,"assessmentsAttempts":null,"questionAttempts":null,"releaseCondition":"80sn3d57-6e58-4554-ad71-f82e2cflsj04","maxRepeat":null}],"status":"PUBLISHED","supportingComponents":[{"abbreviation":"abbr1","alwaysEnabled":false,"assessmentsAttempts":3,"completionNode":true,"exitTicket":false,"icon":"bigIdea-1","id":"80sn3d57-6e58-4554-ad71-f82e2cflsj06","maxRepeat":2,"name":"component 1 UPDATED","order":1,"passingScore":80,"performanceConditions":[],"questionAttempts":{"attempts":3,"hints":false},"releaseCondition":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","type":"CONTENT"}],"templateUuid":"830a4d57-6e58-4554-ad71-f82e2cf9c6b3","eventdate":"2020-08-13"}
        |]
      """.stripMargin
    val expRedshiftJson =
      """
        |[
        |{"at_publisher_id":99999,"at_release_component":"80sn3d57-6e58-4554-ad71-f82e2cflsj06","at_section_type":1,"at_component_name":"component 1 UPDATED","at_question_attempts":3,"at_exit_ticket":false,"at_publisher_name":"john doe","at_name":"name 1 UPDATED","at_always_enabled":false,"at_description":"given description UPDATED","at_icon":"bigIdea-1","at_min":0,"at_completion_node":true,"at_question_attempts_hints":false,"at_max_repeat":2,"at_assessments_attempts":3,"at_activity_type":"INSTRUCTIONAL_LESSON","at_component_uuid":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","at_max":90,"at_order":1,"at_uuid":"830a4d57-6e58-4554-ad71-f82e2cf9c6b3","at_published_date":"2020-08-13T08:15:27.529Z","at_release_condition":"","at_component_type":5,"at_abbreviation":"abbr1","at_status":1,"at_passing_score":80,"at_created_time":"2020-08-13T08:15:27.529Z","at_dw_created_time":"2021-06-23T09:57:57.320Z","at_updated_time":null,"at_dw_updated_time":null},
        |{"at_publisher_id":99999,"at_section_type":2,"at_component_name":"component 1 UPDATED","at_question_attempts":3,"at_exit_ticket":false,"at_publisher_name":"john doe","at_name":"name 1 UPDATED","at_always_enabled":false,"at_description":"given description UPDATED","at_icon":"bigIdea-1","at_completion_node":true,"at_question_attempts_hints":false,"at_max_repeat":2,"at_assessments_attempts":3,"at_activity_type":"INSTRUCTIONAL_LESSON","at_component_uuid":"80sn3d57-6e58-4554-ad71-f82e2cflsj06","at_order":1,"at_uuid":"830a4d57-6e58-4554-ad71-f82e2cf9c6b3","at_published_date":"2020-08-13T08:15:27.529Z","at_release_condition":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","at_component_type":1,"at_abbreviation":"abbr1","at_status":1,"at_passing_score":80,"at_created_time":"2020-08-13T08:15:27.529Z","at_dw_created_time":"2021-06-23T09:57:57.320Z","at_updated_time":null,"at_dw_updated_time":null},
        |{"at_publisher_id":99999,"at_section_type":3,"at_component_name":"component 1 UPDATED","at_exit_ticket":false,"at_publisher_name":"john doe","at_name":"name 1 UPDATED","at_always_enabled":true,"at_description":"given description UPDATED","at_icon":"bigIdea-1","at_completion_node":false,"at_activity_type":"INSTRUCTIONAL_LESSON","at_component_uuid":"20sn3d57-6e58-4554-ad71-f82e2ckasj96","at_order":0,"at_uuid":"830a4d57-6e58-4554-ad71-f82e2cf9c6b3","at_published_date":"2020-08-13T08:15:27.529Z","at_component_type":6,"at_abbreviation":"abbr1","at_status":1,"at_created_time":"2020-08-13T08:15:27.529Z","at_dw_created_time":"2021-06-23T09:57:57.320Z","at_updated_time":null,"at_dw_updated_time":null,"at_question_attempts":null,"at_question_attempts_hints":null,"at_passing_score":null,"at_max_repeat":null,"at_assessments_attempts":null,"at_release_condition":"80sn3d57-6e58-4554-ad71-f82e2cflsj04"},
        |{"at_publisher_id":99999,"at_release_component":"80sn3d57-6e58-4554-ad71-f82e2cflsj06","at_section_type":1,"at_component_name":"component 1","at_question_attempts":2,"at_exit_ticket":false,"at_publisher_name":"john doe","at_name":"name 1","at_always_enabled":false,"at_description":"given description","at_icon":"bigIdea-1","at_min":0,"at_completion_node":true,"at_question_attempts_hints":false,"at_max_repeat":2,"at_assessments_attempts":2,"at_activity_type":"INSTRUCTIONAL_LESSON","at_component_uuid":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","at_max":100,"at_order":1,"at_uuid":"830a4d57-6e58-4554-ad71-f82e2cf9c6b3","at_published_date":"2020-08-13T08:15:25.529Z","at_release_condition":"","at_component_type":5,"at_abbreviation":"abbr1","at_status":4,"at_passing_score":80,"at_created_time":"2020-08-13T08:15:25.529Z","at_dw_created_time":"2021-06-23T09:57:57.320Z","at_updated_time":"2020-08-13T08:15:27.529Z","at_dw_updated_time":null},
        |{"at_publisher_id":99999,"at_section_type":2,"at_component_name":"component 1","at_question_attempts":2,"at_exit_ticket":false,"at_publisher_name":"john doe","at_name":"name 1","at_always_enabled":false,"at_description":"given description","at_icon":"bigIdea-1","at_completion_node":true,"at_question_attempts_hints":false,"at_max_repeat":2,"at_assessments_attempts":2,"at_activity_type":"INSTRUCTIONAL_LESSON","at_component_uuid":"80sn3d57-6e58-4554-ad71-f82e2cflsj06","at_order":1,"at_uuid":"830a4d57-6e58-4554-ad71-f82e2cf9c6b3","at_published_date":"2020-08-13T08:15:25.529Z","at_release_condition":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","at_component_type":1,"at_abbreviation":"abbr1","at_status":4,"at_passing_score":80,"at_created_time":"2020-08-13T08:15:25.529Z","at_dw_created_time":"2021-06-23T09:57:57.320Z","at_updated_time":"2020-08-13T08:15:27.529Z","at_dw_updated_time":null},
        |{"at_publisher_id":99999,"at_section_type":3,"at_component_name":"component 1","at_exit_ticket":false,"at_publisher_name":"john doe","at_name":"name 1","at_always_enabled":true,"at_description":"given description","at_icon":"bigIdea-1","at_completion_node":false,"at_activity_type":"INSTRUCTIONAL_LESSON","at_component_uuid":"20sn3d57-6e58-4554-ad71-f82e2ckasj96","at_order":0,"at_uuid":"830a4d57-6e58-4554-ad71-f82e2cf9c6b3","at_published_date":"2020-08-13T08:15:25.529Z","at_component_type":6,"at_abbreviation":"abbr1","at_status":4,"at_created_time":"2020-08-13T08:15:25.529Z","at_dw_created_time":"2021-06-23T09:57:57.320Z","at_updated_time":"2020-08-13T08:15:27.529Z","at_dw_updated_time":null,"at_question_attempts":null,"at_question_attempts_hints":null,"at_passing_score":null,"at_max_repeat":null,"at_assessments_attempts":null,"at_release_condition":null}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)
    val expDeltaJson =
      """
        |[
        |{"at_publisher_id":99999,"at_name":"name 1 UPDATED","at_publisher_name":"john doe","at_description":"given description UPDATED","supportingComponents":[{"abbreviation":"abbr1","alwaysEnabled":false,"assessmentsAttempts":3,"completionNode":true,"exitTicket":false,"icon":"bigIdea-1","id":"80sn3d57-6e58-4554-ad71-f82e2cflsj06","maxRepeat":2,"name":"component 1 UPDATED","order":1,"passingScore":80,"performanceConditions":[],"questionAttempts":{"attempts":3,"hints":false},"releaseCondition":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","type":"CONTENT"}],"mainComponents":[{"abbreviation":"abbr1","alwaysEnabled":false,"assessmentsAttempts":3,"completionNode":true,"exitTicket":false,"icon":"bigIdea-1","id":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","maxRepeat":2,"name":"component 1 UPDATED","order":1,"passingScore":80,"performanceConditions":[{"max":90,"min":0,"releaseComponent":"80sn3d57-6e58-4554-ad71-f82e2cflsj06"}],"questionAttempts":{"attempts":3,"hints":false},"releaseCondition":"","type":"ASSESSMENT"}],"at_activity_type":"INSTRUCTIONAL_LESSON","at_id":"830a4d57-6e58-4554-ad71-f82e2cf9c6b3","at_published_date":"2020-08-13 08:15:27.529","sideComponents":[{"abbreviation":"abbr1","alwaysEnabled":true,"completionNode":false,"exitTicket":false,"icon":"bigIdea-1","id":"20sn3d57-6e58-4554-ad71-f82e2ckasj96","name":"component 1 UPDATED","order":0,"performanceConditions":[],"type":"KEY_TERM","passingScore":null,"assessmentsAttempts":null,"questionAttempts":null,"releaseCondition":"80sn3d57-6e58-4554-ad71-f82e2cflsj04","maxRepeat":null}],"at_status":1,"at_created_time":"2020-08-13 08:15:27.529Z","at_dw_created_time":"2021-06-23 10:19:48.794Z","at_dw_updated_time":null,"at_updated_time":null},
        |{"at_publisher_id":99999,"at_name":"name 1","at_publisher_name":"john doe","at_description":"given description","supportingComponents":[{"abbreviation":"abbr1","alwaysEnabled":false,"assessmentsAttempts":2,"completionNode":true,"exitTicket":false,"icon":"bigIdea-1","id":"80sn3d57-6e58-4554-ad71-f82e2cflsj06","maxRepeat":2,"name":"component 1","order":1,"passingScore":80,"performanceConditions":[],"questionAttempts":{"attempts":2,"hints":false},"releaseCondition":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","type":"CONTENT"}],"mainComponents":[{"abbreviation":"abbr1","alwaysEnabled":false,"assessmentsAttempts":2,"completionNode":true,"exitTicket":false,"icon":"bigIdea-1","id":"80sn3d57-6e58-4554-ad71-f82e2cflsj03","maxRepeat":2,"name":"component 1","order":1,"passingScore":80,"performanceConditions":[{"max":100,"min":0,"releaseComponent":"80sn3d57-6e58-4554-ad71-f82e2cflsj06"}],"questionAttempts":{"attempts":2,"hints":false},"releaseCondition":"","type":"ASSESSMENT"}],"at_activity_type":"INSTRUCTIONAL_LESSON","at_id":"830a4d57-6e58-4554-ad71-f82e2cf9c6b3","at_published_date":"2020-08-13 08:15:25.529","sideComponents":[{"abbreviation":"abbr1","alwaysEnabled":true,"completionNode":false,"exitTicket":false,"icon":"bigIdea-1","id":"20sn3d57-6e58-4554-ad71-f82e2ckasj96","name":"component 1","order":0,"performanceConditions":[],"type":"KEY_TERM","passingScore":null,"assessmentsAttempts":null,"questionAttempts":null,"releaseCondition":null,"maxRepeat":null}],"at_status":4,"at_created_time":"2020-08-13 08:15:25.529Z","at_dw_created_time":"2021-06-23 10:19:48.794Z","at_dw_updated_time":null,"at_updated_time":"2020-08-13 08:15:27.529Z"}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetCCLActivityTemplateSource,
          value = """
                    |[
                    |{
                    |   "eventType": "ActivityTemplatePublishedEvent",
                    |	  "templateUuid": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
                    |	  "name": "name 1",
                    |	  "description": "given description",
                    |	  "status": "PUBLISHED",
                    |	  "activityType": "INSTRUCTIONAL_LESSON",
                    |	  "dateCreated": "2020-08-13T08:15:25.529",
                    |	  "publishedDate": "2020-08-13T08:15:25.529",
                    |	  "publisherId": 99999,
                    |	  "publisherName": "john doe",
                    |	  "mainComponents": [
                    |	    {
                    |	      "id": "80sn3d57-6e58-4554-ad71-f82e2cflsj03",
                    |	      "order": 1,
                    |	      "name": "component 1",
                    |	      "type": "ASSESSMENT",
                    |	      "abbreviation": "abbr1",
                    |	      "icon": "bigIdea-1",
                    |	      "maxRepeat": 2,
                    |	      "exitTicket": false,
                    |	      "completionNode": true,
                    |	      "alwaysEnabled": false,
                    |	      "passingScore": 80,
                    |	      "assessmentsAttempts": 2,
                    |	      "questionAttempts": {
                    |           "hints": false,
                    |           "attempts": 2
                    |       },
                    |	      "releaseCondition": "",
                    |	      "performanceConditions": [
                    |	        {"releaseComponent": "80sn3d57-6e58-4554-ad71-f82e2cflsj06", "min": 0, "max": 100}
                    |	      ]
                    |	    }
                    |	  ],
                    |	  "supportingComponents": [
                    |	    {
                    |	      "id": "80sn3d57-6e58-4554-ad71-f82e2cflsj06",
                    |	      "order": 1,
                    |	      "name": "component 1",
                    |	      "type": "CONTENT",
                    |	      "abbreviation": "abbr1",
                    |	      "icon": "bigIdea-1",
                    |	      "maxRepeat": 2,
                    |	      "exitTicket": false,
                    |	      "completionNode": true,
                    |	      "alwaysEnabled": false,
                    |	      "passingScore": 80,
                    |	      "assessmentsAttempts": 2,
                    |	      "questionAttempts": {
                    |           "hints": false,
                    |           "attempts": 2
                    |       },
                    |	      "releaseCondition": "80sn3d57-6e58-4554-ad71-f82e2cflsj03",
                    |	      "performanceConditions": []
                    |	    }
                    |	  ],
                    |	  "sideComponents": [
                    |	    {
                    |	      "id": "20sn3d57-6e58-4554-ad71-f82e2ckasj96",
                    |	      "order": 0,
                    |	      "name": "component 1",
                    |	      "type": "KEY_TERM",
                    |	      "abbreviation": "abbr1",
                    |	      "icon": "bigIdea-1",
                    |	      "maxRepeat": null,
                    |	      "exitTicket": false,
                    |	      "completionNode": false,
                    |	      "alwaysEnabled": true,
                    |	      "passingScore": null,
                    |	      "assessmentsAttempts": null,
                    |	      "questionAttempts": null,
                    |	      "releaseCondition": null,
                    |	      "performanceConditions": []
                    |	    }
                    |	  ],
                    |	  "occurredOn": 1597306525992
                    |},
                    |{
                    |   "eventType": "ActivityTemplatePublishedEvent",
                    |	  "templateUuid": "830a4d57-6e58-4554-ad71-f82e2cf9c6b3",
                    |	  "name": "name 1 UPDATED",
                    |	  "description": "given description UPDATED",
                    |	  "status": "PUBLISHED",
                    |	  "activityType": "INSTRUCTIONAL_LESSON",
                    |	  "dateCreated": "2020-08-13T08:15:27.529",
                    |	  "publishedDate": "2020-08-13T08:15:27.529",
                    |	  "publisherId": 99999,
                    |	  "publisherName": "john doe",
                    |	  "mainComponents": [
                    |	    {
                    |	      "id": "80sn3d57-6e58-4554-ad71-f82e2cflsj03",
                    |	      "order": 1,
                    |	      "name": "component 1 UPDATED",
                    |	      "type": "ASSESSMENT",
                    |	      "abbreviation": "abbr1",
                    |	      "icon": "bigIdea-1",
                    |	      "maxRepeat": 2,
                    |	      "exitTicket": false,
                    |	      "completionNode": true,
                    |	      "alwaysEnabled": false,
                    |	      "passingScore": 80,
                    |	      "assessmentsAttempts": 3,
                    |	      "questionAttempts": {
                    |           "hints": false,
                    |           "attempts": 3
                    |       },
                    |	      "releaseCondition": "",
                    |	      "performanceConditions": [
                    |	        {"releaseComponent": "80sn3d57-6e58-4554-ad71-f82e2cflsj06", "min": 0, "max": 90}
                    |	      ]
                    |	    }
                    |	  ],
                    |	  "supportingComponents": [
                    |	    {
                    |	      "id": "80sn3d57-6e58-4554-ad71-f82e2cflsj06",
                    |	      "order": 1,
                    |	      "name": "component 1 UPDATED",
                    |	      "type": "CONTENT",
                    |	      "abbreviation": "abbr1",
                    |	      "icon": "bigIdea-1",
                    |	      "maxRepeat": 2,
                    |	      "exitTicket": false,
                    |	      "completionNode": true,
                    |	      "alwaysEnabled": false,
                    |	      "passingScore": 80,
                    |	      "assessmentsAttempts": 3,
                    |	      "questionAttempts": {
                    |           "hints": false,
                    |           "attempts": 3
                    |       },
                    |	      "releaseCondition": "80sn3d57-6e58-4554-ad71-f82e2cflsj03",
                    |	      "performanceConditions": []
                    |	    }
                    |	  ],
                    |	  "sideComponents": [
                    |	    {
                    |	      "id": "20sn3d57-6e58-4554-ad71-f82e2ckasj96",
                    |	      "order": 0,
                    |	      "name": "component 1 UPDATED",
                    |	      "type": "KEY_TERM",
                    |	      "abbreviation": "abbr1",
                    |	      "icon": "bigIdea-1",
                    |	      "maxRepeat": null,
                    |	      "exitTicket": false,
                    |	      "completionNode": false,
                    |	      "alwaysEnabled": true,
                    |	      "passingScore": null,
                    |	      "assessmentsAttempts": null,
                    |	      "questionAttempts": null,
                    |	      "releaseCondition": "80sn3d57-6e58-4554-ad71-f82e2cflsj04",
                    |	      "performanceConditions": []
                    |	    }
                    |	  ],
                    |	  "occurredOn": 1597306526992
                    |}
                    |]
                  """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquet = sinks.find(_.name === ParquetCCLActivityTemplateSink).get.output
          val redshift = sinks.find(_.name === RedshiftCCLActivityTemplateSink)
            .get.output
          val delta = sinks.find(_.name === DeltaCCLActivityTemplateSink)
            .get.output

          assertSmallDatasetEquality(spark, ActivityTemplateEntity, parquet, expParquetJson)
          assertSmallDatasetEquality(ActivityTemplateEntity, redshift, expRedshiftDf)
          assertSmallDatasetEquality(ActivityTemplateEntity, delta, expDeltaDf)
        }
      )
    }
  }

  def createDfFromJsonWithTimeCols(spark: SparkSession, json: String): DataFrame = {
    createDfFromJson(spark, json)
      .withColumn("at_published_date", col("at_published_date").cast(TimestampType))
      .withColumn("at_created_time", col("at_created_time").cast(TimestampType))
      .withColumn("at_updated_time", col("at_updated_time").cast(TimestampType))
  }
}
