package com.alefeducation.dimensions

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.commons.testutils.SparkSuite
import com.alefeducation.dimensions.tag.TagDimensionTransform
import com.alefeducation.dimensions.tag.TagDimensionTransform.TagMutatedTransformService
import com.alefeducation.util.Resources.getSource
import org.apache.spark.sql.SparkSession
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

class TagDimensionSpec extends SparkSuite{

  val sprk: SparkSession = spark
  val service: SparkBatchService = mock[SparkBatchService]

  val expectedTransformColumns = Set(
    "tag_dw_updated_time",
    "tag_name",
    "tag_association_attach_status",
    "tag_updated_time",
    "tag_association_id",
    "tag_id",
    "tag_association_type",
    "tag_type",
    "tag_status",
    "tag_created_time",
    "tag_dw_created_time",
    "tag_dw_id"
  )

  test("Tag association tagged/untagged events") {

    val incoming =
      """
        |{
        |      "id":"d0aef259-3a4f-4e22-8feb-f579ff1dc534",
        |      "name":"bogor",
        |      "occurredOn":"2020-07-20 02:40:00.0",
        |      "tagId":"3b64d088-6471-4826-9b57-be229451b5e5",
        |      "type":"SCHOOL",
        |      "eventType":"TaggedEvent",
        |      "tenantId":"93e4949d-7eff-4707-9201-dac917a5e013"
        |}""".stripMargin

    val sprk = spark
    import sprk.implicits._

    val incomingDF = spark.read.json(Seq(incoming).toDS())
    when(service.readOptional(getSource(TagMutatedTransformService).head, sprk, extraProps = List(("mergeSchema", "true")))).thenReturn(Some(incomingDF))

    val transformer = new TagDimensionTransform(sprk, service)
    val sinks = transformer.transform()

    val df = sinks.head.get.output

    assert(df.columns.toSet === expectedTransformColumns)

    assert[String](df, "tag_name", "bogor")
    assert[Integer](df, "tag_association_attach_status", 1)
    assert[String](df, "tag_updated_time", null)
    assert[String](df, "tag_association_id", "d0aef259-3a4f-4e22-8feb-f579ff1dc534")
    assert[String](df, "tag_id", "3b64d088-6471-4826-9b57-be229451b5e5")
    assert[Integer](df, "tag_association_type", 1)
    assert[String](df, "tag_type", "SCHOOL")
    assert[Integer](df, "tag_status", 1)
  }
}