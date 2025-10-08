package com.alefeducation.facts

import java.util.TimeZone
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.util.DataFrameEqualityUtils.{assertSmallDatasetEquality, createDfFromJson}
import com.alefeducation.util.Helpers._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.ZonedDateTime

class ConversationOccurredSpec extends SparkSuite {

  trait Setup {
    implicit val transformer = new ConversationOccurred(ConversationOccurredService, spark)
  }

  test("transform in conversation occurred fact successfully") {

    val expParquetJson =
      """
        |[
        |{"answer":"Please ask me about middle school topics in Science.","answerId":"answer-id","conversationId":"conversation-id","createdOn":1547542976768,"curriculum":"curriculum","curriculumId":"curriculum-id","eventDateDw":20190115,"eventType":"ConversationOccurred","gender":"M","grade":"6","gradeId":"grade-id","learningSessionId":"a34cc122-3de0-4f63-a844-9a0a28d68073","mloId":"lo-id","occurredOn":"2019-01-15 09:02:56.768","question":"Hello","school":"XYZ School","schoolId":"school-id","section":"C","sectionId":"section-id","source":"smalltalk","subject":"Science","subjectId":"subject-id","suggestions":["generate","output"],"tenantId":"tenant-id","userId":"user-id","eventdate":"2019-01-15","arabicAnswer":null}
        |]
      """.stripMargin
    val expRedshiftJson =
      """
        |[
        |{"fco_question":"Hello","fco_date_dw_id":20190115,"fco_source":"smalltalk","fco_subject_category":"Science","school_uuid":"school-id","lo_uuid":"lo-id","fco_suggestions":"generate, output","fco_answer_id":"answer-id","fco_learning_session_id":"a34cc122-3de0-4f63-a844-9a0a28d68073","fco_id":"conversation-id","section_uuid":"section-id","fco_answer":"Please ask me about middle school topics in Science.","student_uuid":"user-id","tenant_uuid":"tenant-id","grade_uuid":"grade-id","fco_created_time":"2019-01-15T09:02:56.768Z","fco_dw_created_time":"2020-10-26T07:48:33.460Z","fco_arabic_answer":null,"eventdate":"2019-01-15"}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)
    val expDeltaJson =
      """
        |[
        |{"fco_question":"Hello","fco_date_dw_id":20190115,"fco_source":"smalltalk","fco_subject_category":"Science","fco_school_id":"school-id","fco_lo_id":"lo-id","fco_suggestions":"generate, output","fco_answer_id":"answer-id","fco_learning_session_id":"a34cc122-3de0-4f63-a844-9a0a28d68073","fco_id":"conversation-id","fco_section_id":"section-id","fco_answer":"Please ask me about middle school topics in Science.","fco_student_id":"user-id","fco_tenant_id":"tenant-id","fco_grade_id":"grade-id","fco_created_time":"2019-01-15T09:02:56.768Z","fco_dw_created_time":"2020-10-26T07:51:48.701Z","fco_arabic_answer":null,"eventdate":"2019-01-15"}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetConversationOccurredSource,
          value = s"""
               |[
               |{
               | "eventType":"ConversationOccurred",
               | "tenantId":"tenant-id",
               | "createdOn":1547542976768,
               | "conversationId":"conversation-id",
               | "userId":"user-id",
               | "gender":"M",
               | "school":"XYZ School",
               | "schoolId":"school-id",
               | "grade":"6",
               | "gradeId":"grade-id",
               | "curriculum":"curriculum",
               | "curriculumId":"curriculum-id",
               | "subject":"Science",
               | "subjectId":"subject-id",
               | "section":"C",
               | "sectionId":"section-id",
               | "mloId":"lo-id",
               | "question":"Hello",
               | "answerId":"answer-id",
               | "answer":"Please ask me about middle school topics in Science.",
               | "arabicAnswer":null,
               | "source":"smalltalk",
               | "suggestions":["generate","output"],
               | "eventDateDw": 20190115,
               | "occurredOn": "2019-01-15 09:02:56.768",
               | "learningSessionId": "a34cc122-3de0-4f63-a844-9a0a28d68073"
               |}
               |]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>
          val redshiftDf = sinks.find(_.name == RedshiftConversationOccurredSink).get.output
          val deltaDf = sinks.find(_.name == DeltaConversationOccurredSink).get.output
          val parquetDf = sinks.find(_.name == ParquetConversationOccurredSink).get.output

          assertSmallDatasetEquality(spark, FactConversationOccurredEntity, parquetDf, expParquetJson)
          assertSmallDatasetEquality(FactConversationOccurredEntity, redshiftDf, expRedshiftDf)
          assertSmallDatasetEquality(FactConversationOccurredEntity, deltaDf, expDeltaDf)
        }
      )
    }
  }

  test("transform in conversation occurred fact successfully and truncate to 2046") {
    val fieldStart = "This is a question which should be asked."
    val returnField = (0 to 1000).foldRight("") {
      case (_, acc) => fieldStart + acc
    }
    val cutReturnField = returnField.substring(StartingIndexConversationOccurred, EndIndexConversationOccurred)

    val expParquetJson =
      s"""
        |[
        |{"answer":"$returnField","answerId":1234,"arabicAnswer":"$returnField","conversationId":"conversation-id","createdOn":1547542976768,"curriculum":"curriculum","curriculumId":"curriculum-id","eventDateDw":20190115,"eventType":"ConversationOccurred","gender":"M","grade":"6","gradeId":"grade-id","learningSessionId":"381f1101-be49-4448-a978-ee8b7c73b9dc","mloId":"lo-id","occurredOn":"2019-01-15 09:02:56.768","question":"$returnField","school":"XYZ School","schoolId":"school-id","section":"C","sectionId":"section-id","source":"smalltalk","subject":"Science","subjectId":"subject-id","suggestions":["$returnField","output"],"tenantId":"tenant-id","userId":"user-id","eventdate":"2019-01-15"}
        |]
      """.stripMargin
    val expRedshiftJson =
      s"""
        |[
        |{"fco_question":"$cutReturnField","fco_date_dw_id":20190115,"fco_source":"smalltalk","fco_arabic_answer":"$cutReturnField","fco_subject_category":"Science","school_uuid":"school-id","lo_uuid":"lo-id","fco_suggestions":"$cutReturnField","fco_answer_id":1234,"fco_learning_session_id":"381f1101-be49-4448-a978-ee8b7c73b9dc","fco_id":"conversation-id","section_uuid":"section-id","fco_answer":"$cutReturnField","student_uuid":"user-id","tenant_uuid":"tenant-id","grade_uuid":"grade-id","fco_created_time":"2019-01-15T09:02:56.768Z","fco_dw_created_time":"2020-10-27T03:03:51.694Z","eventdate":"2019-01-15"}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)
    val expDeltaJson =
      s"""
        |[
        |{"fco_question":"$cutReturnField","fco_date_dw_id":20190115,"fco_source":"smalltalk","fco_arabic_answer":"$cutReturnField","fco_subject_category":"Science","fco_school_id":"school-id","fco_lo_id":"lo-id","fco_suggestions":"$cutReturnField","fco_answer_id":1234,"fco_learning_session_id":"381f1101-be49-4448-a978-ee8b7c73b9dc","fco_id":"conversation-id","fco_section_id":"section-id","fco_answer":"$cutReturnField","fco_student_id":"user-id","fco_tenant_id":"tenant-id","fco_grade_id":"grade-id","fco_created_time":"2019-01-15T09:02:56.768Z","fco_dw_created_time":"2020-10-27T03:03:51.694Z","eventdate":"2019-01-15"}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)

    new Setup {

      val fixtures = List(
        SparkFixture(
          key = ParquetConversationOccurredSource,
          value = s"""
               |[
               |{
               | "eventType":"ConversationOccurred",
               | "tenantId":"tenant-id",
               | "createdOn":1547542976768,
               | "conversationId":"conversation-id",
               | "userId":"user-id",
               | "gender":"M",
               | "school":"XYZ School",
               | "schoolId":"school-id",
               | "grade":"6",
               | "gradeId":"grade-id",
               | "curriculum":"curriculum",
               | "curriculumId":"curriculum-id",
               | "subject":"Science",
               | "subjectId":"subject-id",
               | "section":"C",
               | "sectionId":"section-id",
               | "mloId":"lo-id",
               | "question":"$returnField",
               | "answerId":1234,
               |  "answer":"$returnField",
               | "arabicAnswer":"$returnField",
               | "source":"smalltalk",
               | "suggestions":["$returnField","output"],
               | "eventDateDw": 20190115,
               | "occurredOn": "2019-01-15 09:02:56.768",
               | "learningSessionId": "381f1101-be49-4448-a978-ee8b7c73b9dc"
               |}
               |]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquetDf = sinks.find(_.name == ParquetConversationOccurredSink).get.output
          val redshiftDf = sinks.find(_.name == RedshiftConversationOccurredSink).get.output
          val deltaDf = sinks.find(_.name == DeltaConversationOccurredSink).get.output


          assertSmallDatasetEquality(spark, FactConversationOccurredEntity, parquetDf, expParquetJson)
          assertSmallDatasetEquality(FactConversationOccurredEntity, redshiftDf, expRedshiftDf)
          assertSmallDatasetEquality(FactConversationOccurredEntity, deltaDf, expDeltaDf)
        }
      )
    }
  }

  test("match empty suggesstions to n/a") {
    val expParquetJson =
      """
        |[
        |{"answer":"sss","answerId":1234,"arabicAnswer":"sdd","conversationId":"conversation-id","createdOn":1547542976768,"curriculum":"curriculum","curriculumId":"curriculum-id","eventDateDw":20190115,"eventType":"ConversationOccurred","gender":"M","grade":"6","gradeId":"grade-id","learningSessionId":"01f4707c-3f0b-4241-82ec-6606118bef4a","mloId":"lo-id","occurredOn":"2019-01-15 09:02:56.768","question":"dd","school":"XYZ School","schoolId":"school-id","section":"C","sectionId":"section-id","source":"smalltalk","subject":"Science","subjectId":"subject-id","suggestions":[],"tenantId":"tenant-id","userId":"user-id","eventdate":"2019-01-15"}
        |]
      """.stripMargin
    val expRedshiftJson =
      """
        |[
        |{"fco_question":"dd","fco_date_dw_id":20190115,"fco_source":"smalltalk","fco_arabic_answer":"sdd","fco_subject_category":"Science","school_uuid":"school-id","lo_uuid":"lo-id","fco_suggestions":"n/a","fco_answer_id":1234,"fco_learning_session_id":"01f4707c-3f0b-4241-82ec-6606118bef4a","fco_id":"conversation-id","section_uuid":"section-id","fco_answer":"sss","student_uuid":"user-id","tenant_uuid":"tenant-id","grade_uuid":"grade-id","fco_created_time":"2019-01-15T09:02:56.768Z","fco_dw_created_time":"2020-10-27T03:58:54.812Z","eventdate":"2019-01-15"}
        |]
      """.stripMargin
    val expRedshiftDf = createDfFromJsonWithTimeCols(spark, expRedshiftJson)
    val expDeltaJson =
      """
        |[
        |{"fco_question":"dd","fco_date_dw_id":20190115,"fco_source":"smalltalk","fco_arabic_answer":"sdd","fco_subject_category":"Science","fco_school_id":"school-id","fco_lo_id":"lo-id","fco_suggestions":"n/a","fco_answer_id":1234,"fco_learning_session_id":"01f4707c-3f0b-4241-82ec-6606118bef4a","fco_id":"conversation-id","fco_section_id":"section-id","fco_answer":"sss","fco_student_id":"user-id","fco_tenant_id":"tenant-id","fco_grade_id":"grade-id","fco_created_time":"2019-01-15T09:02:56.768Z","fco_dw_created_time":"2020-10-27T03:58:54.812Z","eventdate":"2019-01-15"}
        |]
      """.stripMargin
    val expDeltaDf = createDfFromJsonWithTimeCols(spark, expDeltaJson)
    new Setup {
      val fixtures = List(
        SparkFixture(
          key = ParquetConversationOccurredSource,
          value = s"""
               |[
               |{
               | "eventType":"ConversationOccurred",
               | "tenantId":"tenant-id",
               | "createdOn":1547542976768,
               | "conversationId":"conversation-id",
               | "userId":"user-id",
               | "gender":"M",
               | "school":"XYZ School",
               | "schoolId":"school-id",
               | "grade":"6",
               | "gradeId":"grade-id",
               | "curriculum":"curriculum",
               | "curriculumId":"curriculum-id",
               | "subject":"Science",
               | "subjectId":"subject-id",
               | "section":"C",
               | "sectionId":"section-id",
               | "mloId":"lo-id",
               | "question":"dd",
               | "answerId":1234,
               |  "answer":"sss",
               | "arabicAnswer":"sdd",
               | "source":"smalltalk",
               | "suggestions":[],
               | "eventDateDw": 20190115,
               | "occurredOn": "2019-01-15 09:02:56.768",
               | "learningSessionId": "01f4707c-3f0b-4241-82ec-6606118bef4a"
               |}
               |]
      """.stripMargin
        )
      )
      withSparkFixtures(
        fixtures, { sinks =>

          val parquetDf = sinks.find(_.name == ParquetConversationOccurredSink).get.output
          val redshiftDf = sinks.find(_.name == RedshiftConversationOccurredSink).get.output
          val deltaDf = sinks.find(_.name == DeltaConversationOccurredSink).get.output

          assertSmallDatasetEquality(spark, FactConversationOccurredEntity, parquetDf, expParquetJson)
          assertSmallDatasetEquality(FactConversationOccurredEntity, redshiftDf, expRedshiftDf)
          assertSmallDatasetEquality(FactConversationOccurredEntity, deltaDf, expDeltaDf)
        }
      )
    }
  }

  def createDfFromJsonWithTimeCols(spark: SparkSession, json: String): DataFrame = {
    createDfFromJson(spark, json)
      .withColumn("fco_created_time", col("fco_created_time").cast(TimestampType))
  }

}
