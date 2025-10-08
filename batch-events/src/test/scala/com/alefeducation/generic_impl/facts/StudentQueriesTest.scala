package com.alefeducation.generic_impl.facts

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.generic_impl.GenericFactTransform
import org.apache.spark.sql.SparkSession
import org.scalatestplus.mockito.MockitoSugar.mock

class StudentQueriesTest extends SparkSuite with BaseDimensionSpec {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should construct transformation data frame for fact jobs when only StudentQuerySubmittedEvent is present") {
    val updatedValue =
      """
        |[
        |{
        |  "_load_date": "20250226",
        |  "_app_tenant": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "_load_time": 1740571617563,
        |  "_source_type": "ALEF-KAFKA",
        |  "_source_app": "NEXTGEN",
        |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
        |  "eventType": "StudentQuerySubmittedEvent",
        |  "hasScreenshot": true,
        |  "queryId": "d15ec592-bdaf-43e2-aa0e-9136f73dab9f",
        |  "is_follow_up": false,
        |  "genSubject": "English",
        |  "schoolId": "fc604213-e624-4502-a48f-db3f1f1d7667",
        |  "mloId": "1decf64a-bc64-4cb6-bbbe-000000033570",
        |  "activityTitle": "Strengthening Descriptions",
        |  "academicYearId": "65f2d127-1fa0-427f-bd7d-6223d8384274",
        |  "activityId": "1decf64a-bc64-4cb6-bbbe-000000033570",
        |  "materialType": "CORE",
        |  "occurredOn": 1737549780732,
        |  "activityType": "INSTRUCTIONAL_LESSON",
        |  "langCode": "en",
        |  "crumbTitle": "Big Idea",
        |  "messageId": "7a635228-3165-4aeb-9b55-ca4c97c1b8e4",
        |  "sectionId": "db35fd1f-2273-4479-90c0-ee6d53b5341c",
        |  "classId": "18933e25-531a-4609-8109-b9a374203b3f",
        |  "gradeId": "c59d283f-445b-4814-90bd-29d408cb51c0",
        |  "studentId": "ac542ed0-06b0-46df-8605-bdf859eb7287"
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "dw_id",
      "created_time",
      "dw_created_time",
      "date_dw_id",
      "_trace_id",
      "event_type",
      "message_id",
      "query_id",
      "tenant_id",
      "school_id",
      "grade_id",
      "class_id",
      "section_id",
      "teacher_id",
      "academic_year_id",
      "student_id",
      "activity_id",
      "activity_title",
      "activity_type",
      "can_student_reply",
      "with_audio",
      "with_text",
      "material_type",
      "crumb_title",
      "gen_subject",
      "lang_code",
      "is_follow_up",
      "has_screenshot",
      "eventdate"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform("student-queries-transform")
    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    val df = transformer.transform(Map(
      "bronze-student-queries-source" -> Some(inputDf)
    ), 1001).getOrElse(sprk.emptyDataFrame)

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)
    assert[String](df, "_trace_id", "1dc711b3-96e1-47ab-8376-8427e211155c")
    assert[String](df, "event_type", "StudentQuerySubmittedEvent")
    assert[String](df, "tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](df, "school_id", "fc604213-e624-4502-a48f-db3f1f1d7667")
    assert[String](df, "teacher_id", null)
    assert[String](df, "created_time", "2025-01-22 12:43:00.732")
    assert[Int](df, "dw_id", 1001)
    assert[String](df, "date_dw_id", "20250122")
    assert[String](df, "eventdate", "2025-01-22")
  }

  test("should construct transformation data frame for fact jobs when only TeacherResponseSubmittedEvent is present") {
    val updatedValue =
      """
        |[
        |{
        |  "_load_date": "20250226",
        |  "_app_tenant": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "_load_time": 1740571617563,
        |  "_source_type": "ALEF-KAFKA",
        |  "_source_app": "NEXTGEN",
        |  "_trace_id": "f700b06a-938b-4195-8c2b-da67634d4f81",
        |  "eventType": "TeacherResponseSubmittedEvent",
        |  "queryId": "d15ec592-bdaf-43e2-aa0e-9136f73dab9f",
        |  "teacherId": "d7f87f7e-5dbc-4fa9-b107-e36bfdd0a70f",
        |  "schoolId": "fc604213-e624-4502-a48f-db3f1f1d7667",
        |  "withAudio": false,
        |  "academicYearId": "65f2d127-1fa0-427f-bd7d-6223d8384274",
        |  "eventId": "357e1463-a79a-4237-975c-02dc255613f2",
        |  "occurredOn": 1737549828331,
        |  "messageId": "ba13149c-434f-4759-80ff-ca275e0488bc",
        |  "sectionId": "db35fd1f-2273-4479-90c0-ee6d53b5341c",
        |  "canStudentReply": true,
        |  "classId": "18933e25-531a-4609-8109-b9a374203b3f",
        |  "gradeId": "c59d283f-445b-4814-90bd-29d408cb51c0",
        |  "withText": true,
        |  "studentId": "ac542ed0-06b0-46df-8605-bdf859eb7287"
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "dw_id",
      "created_time",
      "dw_created_time",
      "date_dw_id",
      "_trace_id",
      "event_type",
      "message_id",
      "query_id",
      "tenant_id",
      "school_id",
      "grade_id",
      "class_id",
      "section_id",
      "teacher_id",
      "academic_year_id",
      "student_id",
      "activity_id",
      "activity_title",
      "activity_type",
      "can_student_reply",
      "with_audio",
      "with_text",
      "material_type",
      "crumb_title",
      "gen_subject",
      "lang_code",
      "is_follow_up",
      "has_screenshot",
      "eventdate"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform("student-queries-transform")
    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    val df = transformer.transform(Map(
      "bronze-student-queries-source" -> Some(inputDf)
    ), 1001).getOrElse(sprk.emptyDataFrame)

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)
    assert[String](df, "_trace_id", "f700b06a-938b-4195-8c2b-da67634d4f81")
    assert[String](df, "event_type", "TeacherResponseSubmittedEvent")
    assert[String](df, "tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](df, "school_id", "fc604213-e624-4502-a48f-db3f1f1d7667")
    assert[String](df, "teacher_id", "d7f87f7e-5dbc-4fa9-b107-e36bfdd0a70f")
    assert[String](df, "created_time", "2025-01-22 12:43:48.331")
    assert[Int](df, "dw_id", 1001)
    assert[String](df, "date_dw_id", "20250122")
    assert[String](df, "eventdate", "2025-01-22")
  }

  test("should construct transformation data frame for fact jobs when both events present") {
    val updatedValue =
      """
        |[
        |{
        |  "_load_date": "20250226",
        |  "_app_tenant": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "_load_time": 1740571617563,
        |  "_source_type": "ALEF-KAFKA",
        |  "_source_app": "NEXTGEN",
        |  "_trace_id": "1dc711b3-96e1-47ab-8376-8427e211155c",
        |  "eventType": "StudentQuerySubmittedEvent",
        |  "hasScreenshot": true,
        |  "queryId": "d15ec592-bdaf-43e2-aa0e-9136f73dab9f",
        |  "is_follow_up": false,
        |  "genSubject": "English",
        |  "schoolId": "fc604213-e624-4502-a48f-db3f1f1d7667",
        |  "mloId": "1decf64a-bc64-4cb6-bbbe-000000033570",
        |  "activityTitle": "Strengthening Descriptions",
        |  "academicYearId": "65f2d127-1fa0-427f-bd7d-6223d8384274",
        |  "activityId": "1decf64a-bc64-4cb6-bbbe-000000033570",
        |  "materialType": "CORE",
        |  "occurredOn": 1737549780732,
        |  "activityType": "INSTRUCTIONAL_LESSON",
        |  "langCode": "en",
        |  "crumbTitle": "Big Idea",
        |  "messageId": "7a635228-3165-4aeb-9b55-ca4c97c1b8e4",
        |  "sectionId": "db35fd1f-2273-4479-90c0-ee6d53b5341c",
        |  "classId": "18933e25-531a-4609-8109-b9a374203b3f",
        |  "gradeId": "c59d283f-445b-4814-90bd-29d408cb51c0",
        |  "studentId": "ac542ed0-06b0-46df-8605-bdf859eb7287"
        |},
        |{
        |  "_load_date": "20250226",
        |  "_app_tenant": "93e4949d-7eff-4707-9201-dac917a5e013",
        |  "_load_time": 1740571617563,
        |  "_source_type": "ALEF-KAFKA",
        |  "_source_app": "NEXTGEN",
        |  "_trace_id": "f700b06a-938b-4195-8c2b-da67634d4f81",
        |  "eventType": "TeacherResponseSubmittedEvent",
        |  "queryId": "d15ec592-bdaf-43e2-aa0e-9136f73dab9f",
        |  "teacherId": "d7f87f7e-5dbc-4fa9-b107-e36bfdd0a70f",
        |  "schoolId": "fc604213-e624-4502-a48f-db3f1f1d7667",
        |  "withAudio": false,
        |  "academicYearId": "65f2d127-1fa0-427f-bd7d-6223d8384274",
        |  "eventId": "357e1463-a79a-4237-975c-02dc255613f2",
        |  "occurredOn": 1737549828331,
        |  "messageId": "ba13149c-434f-4759-80ff-ca275e0488bc",
        |  "sectionId": "db35fd1f-2273-4479-90c0-ee6d53b5341c",
        |  "canStudentReply": true,
        |  "classId": "18933e25-531a-4609-8109-b9a374203b3f",
        |  "gradeId": "c59d283f-445b-4814-90bd-29d408cb51c0",
        |  "withText": true,
        |  "studentId": "ac542ed0-06b0-46df-8605-bdf859eb7287"
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "dw_id",
      "created_time",
      "dw_created_time",
      "date_dw_id",
      "_trace_id",
      "event_type",
      "message_id",
      "query_id",
      "tenant_id",
      "school_id",
      "grade_id",
      "class_id",
      "section_id",
      "teacher_id",
      "academic_year_id",
      "student_id",
      "activity_id",
      "activity_title",
      "activity_type",
      "can_student_reply",
      "with_audio",
      "with_text",
      "material_type",
      "crumb_title",
      "gen_subject",
      "lang_code",
      "is_follow_up",
      "has_screenshot",
      "eventdate"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform("student-queries-transform")
    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    val df = transformer.transform(Map(
      "bronze-student-queries-source" -> Some(inputDf)
    ), 1001).getOrElse(sprk.emptyDataFrame)

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 2)
    assert[String](df, "_trace_id", "1dc711b3-96e1-47ab-8376-8427e211155c")
    assert[String](df, "event_type", "StudentQuerySubmittedEvent")
    assert[String](df, "tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](df, "school_id", "fc604213-e624-4502-a48f-db3f1f1d7667")
    assert[String](df, "teacher_id", null)
    assert[String](df, "created_time", "2025-01-22 12:43:00.732")
    assert[Int](df, "dw_id", 1001)
    assert[String](df, "date_dw_id", "20250122")
    assert[String](df, "eventdate", "2025-01-22")
  }
}
