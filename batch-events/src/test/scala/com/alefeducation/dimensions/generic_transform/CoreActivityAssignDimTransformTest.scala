package com.alefeducation.dimensions.generic_transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.pathway_target.PathwayTargetsTransform
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import java.sql.Timestamp

class CoreActivityAssignDimTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val updatedValue =
    """
      |[
      |{
      |     "eventType": "CoreAdditionalResourceAssignedEvent",
      |     "tenantId": "tenant-id-1",
      |     "id": "3ec89fa0-8902-40b6-9b7b-094f25ae331c",
      |	   	"learnerId": "0ac31190-123e-4fb6-9837-185336b48ef5",
      |	    "courseId": "390c33c8-5ffb-499f-88f1-b2912a9ae45e",
      |	    "classId": "016abb17-5dd8-4166-9459-7be5a414f359",
      |	    "courseType": "CORE",
      |	    "academicYearTag": "2024-2025",
      |	    "activities": [
      |	    	"62bae0ef-3cf0-4f3b-9549-000000104137"
      |	    ],
      |	    "dueDateStart": "2024-12-30",
      |	    "dueDateEnd": null,
      |	    "resourceInfo": [
      |	    	{
      |	    		"activityId": "62bae0ef-3cf0-4f3b-9549-000000104137",
      |	    		"progressStatus": "COMPLETED",
      |	    		"activityType": "ACTIVITY"
      |	    	},
      |	    	{
      |	    		"activityId": "activity-id-2",
      |	    		"progressStatus": "COMPLETED",
      |	    		"activityType": "ACTIVITY"
      |	    	}
      |	    ],
      |	    "assignedBy": "ae19d572-f6b3-438d-8e35-8b91517659f2",
      |	    "assignedOn": "2024-12-31T12:29:39.40801462",
      |	    "uuid": "bd8be752-1600-4bfc-baf3-14cc10d4de73",
      |	    "occurredOn": "2024-12-31T12:29:39.458"
      |},
      |{
      |     "eventType": "CoreAdditionalResourceAssignedEvent",
      |     "tenantId": "tenant-id-1",
      |     "id": "3ec89fa0-8902-40b6-9b7b-094f25ae331c",
      |	   	"learnerId": "0ac31190-123e-4fb6-9837-185336b48ef5",
      |	    "courseId": "390c33c8-5ffb-499f-88f1-b2912a9ae45e",
      |	    "classId": "016abb17-5dd8-4166-9459-7be5a414f359",
      |	    "courseType": "CORE",
      |	    "academicYearTag": "2024-2025",
      |	    "activities": [
      |	    	"62bae0ef-3cf0-4f3b-9549-000000104137"
      |	    ],
      |	    "dueDateStart": "2024-12-30",
      |	    "dueEndDate": null,
      |	    "resourceInfo": [
      |	    	{
      |	    		"activityId": "62bae0ef-3cf0-4f3b-9549-000000104137",
      |	    		"progressStatus": "COMPLETED",
      |	    		"activityType": "ACTIVITY"
      |	    	},
      |	    	{
      |	    		"activityId": "activity-id-2",
      |	    		"progressStatus": "COMPLETED",
      |	    		"activityType": "ACTIVITY"
      |	    	}
      |	    ],
      |	    "assignedBy": "ae19d572-f6b3-438d-8e35-8b91517659f2",
      |	    "assignedOn": "2024-12-31T13:29:39.40801462",
      |	    "uuid": "bd8be752-1600-4bfc-baf3-14cc10d4de73",
      |	    "occurredOn": "2024-12-31T13:29:39.458"
      |}
      |]
      |""".stripMargin

  test("should construct pathway target dimension dataframe when Pathway events mutated event flows") {
    val expectedColumns = Set(
      "cta_dw_id",
      "cta_event_type",
      "cta_id",
      "cta_status",
      "cta_active_until",
      "cta_action_time",
      "cta_class_id",
      "cta_course_id",
      "cta_created_time",
      "cta_dw_created_time",
      "cta_tenant_id",
      "cta_student_id",
      "cta_ay_tag",
      "cta_activity_id",
      "cta_activity_type",
      "cta_end_date",
      "cta_teacher_id",
      "cta_progress_status",
      "cta_start_date"
    )

    val sprk = spark
    import sprk.implicits._
    val transformer = new GenericSCD2Transformation(sprk, service, "core-activity-assign-transform")

    val updatedDF = spark.read.json(Seq(updatedValue).toDS())
    when(service.readOptional("parquet-core-activity-assign-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(updatedDF))
    val sinks = transformer.transform()

    val df = sinks.filter(_.name == "transformed-core-activity-assign").head.output
    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 4)
    assert[String](df, "cta_action_time", "2024-12-31 12:29:39.408014")
    assert[String](df, "cta_class_id", "016abb17-5dd8-4166-9459-7be5a414f359")
    assert[String](df, "cta_student_id", "0ac31190-123e-4fb6-9837-185336b48ef5")
    assert[String](df, "cta_course_id", "390c33c8-5ffb-499f-88f1-b2912a9ae45e")
    assert[String](df, "cta_tenant_id", "tenant-id-1")
    assert[String](df, "cta_ay_tag", "2024-2025")
    assert[String](df, "cta_id", "3ec89fa0-8902-40b6-9b7b-094f25ae331c")
    assert[String](df, "cta_activity_id", "62bae0ef-3cf0-4f3b-9549-000000104137")
    assert[String](df, "cta_activity_type", "ACTIVITY")
    assert[String](df, "cta_teacher_id", "ae19d572-f6b3-438d-8e35-8b91517659f2")
    assert[String](df, "cta_progress_status", "COMPLETED")
    assert[Timestamp](df, "cta_start_date", Timestamp.valueOf("2024-12-30 00:00:00"))
    assert[String](df, "cta_end_date", null)
    assert[String](df, "cta_created_time", "2024-12-31 12:29:39.458")
  }
}
