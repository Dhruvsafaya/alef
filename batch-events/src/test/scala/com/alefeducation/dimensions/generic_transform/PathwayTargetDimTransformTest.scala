package com.alefeducation.dimensions.generic_transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.pathway_target.PathwayTargetsTransform
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class PathwayTargetDimTransformTest extends SparkSuite {
  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val updatedValue =
    """
      |[
      |{
      |     "eventType": "PathwayTargetMutatedEvent",
      |	    "uuid": "2d9b438c-119a-44ec-aba3-10a71y7e285q",
      |     "pathwayTargetId": "31dca798-fe97-47eb-8c4e-14f053b1e300",
      |     "pathwayId": "6d9b438c-119a-48ec-aba3-c0a71c7e284d",
      |     "classId": "51dda788-fe97-47eb-8c4e-34f053b1e777",
      |     "gradeId": "b0b2637a-d452-4b99-88cd-0294e95ddb76",
      |	    "schoolId": "096d5c24-04bf-4522-8f53-63ea2f561d05",
      |	    "tenantId": "096d5c24-04bf-4522-8f53-63ea2f561d05",
      |	    "startDate": "2023-09-27",
      |	    "endDate": "2023-09-27",
      |     "teacherId": "0413e4ea-448f-4aba-bbda-b29caad87508",
      |     "targetStatus": "CREATED",
      |     "occurredOn": "2021-06-23 05:30:25.921"
      |},
      |{
      |     "eventType": "PathwayTargetMutatedEvent",
      |	    "uuid": "2d9b438c-119a-44ec-aba3-10a71y7e285q",
      |     "pathwayTargetId"	: "31dca798-fe97-47eb-8c4e-14f053b1e300",
      |     "pathwayId" : "6d9b438c-119a-48ec-aba3-c0a71c7e284d",
      |     "classId": "51dda788-fe97-47eb-8c4e-34f053b1e777",
      |     "gradeId": "b0b2637a-d452-4b99-88cd-0294e95ddb76",
      |     "tenantId": "096d5c24-04bf-4522-8f53-63ea2f561d05",
      |	    "schoolId": "096d5c24-04bf-4522-8f53-63ea2f561d05",
      |	    "startDate": "2023-09-27",
      |	    "endDate": "2023-09-27",
      |     "teacherId": "0413e4ea-448f-4aba-bbda-b29caad87508",
      |     "targetStatus": "CONCLUDED",
      |     "occurredOn": "2021-06-23 05:43:25.921"
      |},
      |{
      |     "eventType": "PathwayTargetMutatedEvent",
      |	    "uuid": "2d9b438c-119a-44ec-aba3-10a71y7e285q",
      |     "pathwayTargetId": "31dca798-fe97-47eb-8c4e-14f053b1e300",
      |     "pathwayId" : "6d9b438c-119a-48ec-aba3-c0a71c7e284d",
      |     "classId": "51dda788-fe97-47eb-8c4e-34f053b1e777",
      |     "gradeId": "b0b2637a-d452-4b99-88cd-0294e95ddb76",
      |     "tenantId": "096d5c24-04bf-4522-8f53-63ea2f561d05",
      |	    "schoolId": "096d5c24-04bf-4522-8f53-63ea2f561d05",
      |	    "startDate": "2023-09-27",
      |	    "endDate": "2023-09-27",
      |     "teacherId": "0413e4ea-448f-4aba-bbda-b29caad87508",
      |     "targetStatus": "DELETED",
      |     "occurredOn": "2021-06-23 05:53:25.921"
      |}
      |]
      |""".stripMargin

  test("should construct pathway target dimension dataframe when Pathway events mutated event flows") {
    val expectedColumns = Set(
      "pt_dw_id",
      "pt_id",
      "pt_created_time",
      "pt_dw_created_time",
      "pt_pathway_id",
      "pt_teacher_id",
      "pt_end_date",
      "pt_school_id",
      "pt_target_id",
      "pt_tenant_id",
      "pt_start_date",
      "pt_class_id",
      "pt_grade_id",
      "pt_target_state",
      "pt_status",
      "pt_active_until"
    )

    val sprk = spark
    import sprk.implicits._
    val transformer = new PathwayTargetsTransform(sprk, service, "pathway-target-mutated-transform")

    val updatedDF = spark.read.json(Seq(updatedValue).toDS())
    when(service.readOptional("parquet-pathway-target-mutated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(updatedDF))
    val sinks = transformer.transform()

    val df = sinks.filter(_.name == "transformed-pathway-target").head.output
    assert(df.columns.toSet === expectedColumns)
    assert(df.count() == 3)
    assert[String](df, "pt_pathway_id", "6d9b438c-119a-48ec-aba3-c0a71c7e284d")
    assert[String](df, "pt_teacher_id", "0413e4ea-448f-4aba-bbda-b29caad87508")
    assert[String](df, "pt_end_date", "2023-09-27")
    assert[String](df, "pt_school_id", "096d5c24-04bf-4522-8f53-63ea2f561d05")
    assert[String](df, "pt_target_id", "31dca798-fe97-47eb-8c4e-14f053b1e300")
    assert[String](df, "pt_tenant_id", "096d5c24-04bf-4522-8f53-63ea2f561d05")
    assert[String](df, "pt_start_date", "2023-09-27")
    assert[String](df, "pt_class_id", "51dda788-fe97-47eb-8c4e-34f053b1e777")
    assert[String](df, "pt_grade_id", "b0b2637a-d452-4b99-88cd-0294e95ddb76")
    assert[String](df, "pt_created_time", "2021-06-23 05:30:25.921")
    assert[String](df, "pt_target_state", "CREATED")
    assert[Int](df, "pt_status", 2)
  }

  test("should construct rel_dw_id_mappings dataframe when Pathway events mutated event flows") {
    val sprk = spark
    import sprk.implicits._

    val relTransformer = new GenericRelMappingTransformation(sprk, service, "rel-pathway-target-transform")

    val updatedDF = spark.read.json(Seq(updatedValue).toDS())
    when(service.readOptional("parquet-pathway-target-mutated-source", sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(updatedDF))

    val relSinks = relTransformer.transform()

    val relDf = relSinks.filter(_.name == "transformed-rel-pathway-target").head.output
    assert(relDf.count() == 1)
    assert[String](relDf, "id", "31dca798-fe97-47eb-8c4e-14f053b1e300")
    assert[String](relDf, "entity_type", "pathway_target")
  }
}
