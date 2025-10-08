package com.alefeducation.generic_impl.facts

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.BaseDimensionSpec
import com.alefeducation.generic_impl.GenericFactTransform
import org.apache.spark.sql.SparkSession
import org.scalatestplus.mockito.MockitoSugar.mock

class TeacherTaskCenterTest extends SparkSuite with BaseDimensionSpec {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  test("should construct transformation data frame for fact jobs with simple mapping inside configuration") {
    val updatedValue =
      """
        |[
        |{
        |   "eventType":"TeacherTaskForceCompletedEvent",
        |	  "_trace_id":"trace-id-1",
        |	  "_app_tenant":"93e4949d-7eff-4707-9201-dac917a5e013",
        |   "purchaseId": "8b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |   "eventId": "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c",
        |   "type": "AVATAR",
        |   "taskId": "9184acd2-02f7-49e5-9f4e-f0c034188e6b",
        |   "teacherId": "2b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |   "schoolId": "7b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        |   "classId": "3b8cf877-c6e8-4450-aaaf-2333a92abb3e",
        | 	"occurredOn": 1624426405921
        |}
        |]
        |""".stripMargin

    val expectedColumns = Set(
      "created_time",
      "dw_created_time",
      "date_dw_id",
      "eventdate",
      "dw_id",
      "event_type",
      "_trace_id",
      "event_id",
      "task_id",
      "task_type",
      "school_id",
      "class_id",
      "teacher_id",
      "tenant_id"
    )

    val sprk = spark
    import sprk.implicits._

    val transformer = new GenericFactTransform("teacher-task-center-transform")
    val inputDf = spark.read.json(Seq(updatedValue).toDS())
    val df = transformer.transform(Map(
      "bronze-teacher-task-center-source" -> Some(inputDf)
    ), 1001).getOrElse(sprk.emptyDataFrame)

    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 1)
    assert[String](df, "event_id", "3d7ac395-cfe6-4ad6-8ac7-f792f7d2cb0c")
    assert[String](df, "_trace_id", "trace-id-1")
    assert[String](df, "event_type", "TeacherTaskForceCompletedEvent")
    assert[String](df, "tenant_id", "93e4949d-7eff-4707-9201-dac917a5e013")
    assert[String](df, "school_id", "7b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[String](df, "teacher_id", "2b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[String](df, "created_time", "2021-06-23 05:33:25.921")
    assert[String](df, "task_id", "9184acd2-02f7-49e5-9f4e-f0c034188e6b")
    assert[String](df, "task_type", "AVATAR")
    assert[String](df, "class_id", "3b8cf877-c6e8-4450-aaaf-2333a92abb3e")
    assert[Int](df, "dw_id", 1001)
    assert[String](df, "date_dw_id", "20210623")
    assert[String](df, "eventdate", "2021-06-23")
  }
}
