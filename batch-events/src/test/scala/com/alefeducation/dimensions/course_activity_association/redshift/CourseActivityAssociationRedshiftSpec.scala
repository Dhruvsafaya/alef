package com.alefeducation.dimensions.course_activity_association.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.course_activity_association.redshift.CourseActivityAssociationRedshift.CourseActivityAssociationRedshiftService
import com.alefeducation.dimensions.course_activity_association.transform.CourseActivityAssociationTransform.CourseActivityAssociationEntity
import com.alefeducation.service.DataSink
import com.alefeducation.util.DataFrameEqualityUtils.{assertSmallDatasetEquality, createDfFromJson}
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.StringUtilities.replaceSpecChars
import org.mockito.Mockito.when
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import org.scalatestplus.mockito.MockitoSugar.mock

class CourseActivityAssociationRedshiftSpec extends SparkSuite with Matchers {

  val service: SparkBatchService = mock[SparkBatchService]

  private val TransformedMsg: String =
    """
      |{
      |   "caa_activity_type":2,
      |   "caa_attach_status":4,
      |   "caa_status":1,
      |   "caa_activity_id":"2de5617a-5b60-41f7-b321-debdb96a5701",
      |   "caa_container_id":"fcd07e79-d13e-4c13-9af8-eb80d2ec025b",
      |   "caa_course_id":"8fd413c0-7b40-4b92-8825-cfb9d21e7796",
      |   "caa_course_version":"1.0",
      |   "caa_activity_index":9,
      |   "caa_created_time":"2023-01-10 06:36:09.319",
      |   "caa_dw_created_time":"2023-02-08 06:39:14.431",
      |   "caa_dw_updated_time":null
      |}
      |""".stripMargin

  test("should construct course activity dataframe for save to redshift") {
    val sprk = spark
    import sprk.implicits._

    val sourceName = getSource(CourseActivityAssociationRedshiftService).head
    val sinkName = getSink(CourseActivityAssociationRedshiftService).head

    val transformer = new CourseActivityAssociationRedshift(sprk, service)
    val inputDF = spark.read.json(Seq(TransformedMsg).toDS())
    when(service.readOptional(sourceName, sprk)).thenReturn(Some(inputDF))

    val sink: DataSink = transformer.transform(sourceName, sinkName).get.asInstanceOf[DataSink]

    sink.options("dbtable") shouldBe "rs_schema.staging_dim_course_activity_association"

    replaceSpecChars(sink.options("postactions")) shouldBe replaceSpecChars(
      """begin transaction;
        |CREATE TEMPORARY TABLE earliest as select * from rs_schema.staging_dim_course_activity_association where concat(concat(caa_activity_id, caa_course_id), cast(caa_created_time as varchar)) in (select id from (select concat(concat(caa_activity_id, caa_course_id), cast(caa_created_time as varchar)) id, dense_rank() over (partition by caa_activity_id, caa_course_id order by caa_created_time) rnk from rs_schema.staging_dim_course_activity_association) a where a.rnk = 1);;
        |
        |UPDATE rs_stage_schema.rel_course_activity_association
        |SET caa_status = '2', caa_updated_time = earliest.caa_created_time, caa_dw_updated_time = earliest.caa_dw_created_time
        |FROM earliest
        |WHERE rel_course_activity_association.caa_activity_id = earliest.caa_activity_id and rel_course_activity_association.caa_course_id = earliest.caa_course_id AND
        | rel_course_activity_association.caa_status <> '2' AND
        | rel_course_activity_association.caa_created_time < earliest.caa_created_time;
        |
        |UPDATE rs_schema.dim_course_activity_association
        |SET caa_status = '2', caa_updated_time = earliest.caa_created_time, caa_dw_updated_time = earliest.caa_dw_created_time
        |FROM earliest
        |WHERE dim_course_activity_association.caa_activity_id = earliest.caa_activity_id and dim_course_activity_association.caa_course_id = earliest.caa_course_id AND
        | dim_course_activity_association.caa_status <> '2' AND
        | dim_course_activity_association.caa_created_time < earliest.caa_created_time;
        |
        |insert into rs_stage_schema.rel_course_activity_association (caa_activity_id, caa_activity_index, caa_activity_type, caa_attach_status, caa_container_id, caa_course_id, caa_course_version, caa_created_time, caa_dw_created_time, caa_dw_updated_time, caa_status) select caa_activity_id, caa_activity_index, caa_activity_type, caa_attach_status, caa_container_id, caa_course_id, caa_course_version, caa_created_time, caa_dw_created_time, caa_dw_updated_time, caa_status from rs_schema.staging_dim_course_activity_association
        | where not exists (select 1 from rs_stage_schema.rel_course_activity_association where rel_course_activity_association.caa_activity_id = staging_dim_course_activity_association.caa_activity_id and rel_course_activity_association.caa_course_id = staging_dim_course_activity_association.caa_course_id and rel_course_activity_association.caa_status = '1' and rel_course_activity_association.caa_created_time = staging_dim_course_activity_association.caa_created_time);
        |drop table rs_schema.staging_dim_course_activity_association;
        |end transaction""".stripMargin
    )

    assertSmallDatasetEquality(CourseActivityAssociationEntity, sink.output, createDfFromJson(spark, TransformedMsg))
  }

}
