package com.alefeducation.dimensions.tag

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.tag.TagDimensionTransform._
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.{BatchTransformerUtility, SparkSessionUtils}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}


class TagDimensionTransform(val session: SparkSession, val service: SparkBatchService) {

  import BatchTransformerUtility._

  val orderByField: String = "occurredOn"

  val groupKey: List[String] = List(
    "id",
    "tagId",
    "type"
  )

  def transform(): List[Option[Sink]] = {
    val tagSourceDf: Option[DataFrame] = service.readOptional(getSource(TagMutatedTransformService).head, session, extraProps = List(("mergeSchema", "true")))

    val startId = service.getStartIdUpdateStatus(TagKey)

    val tagDf = tagSourceDf.map(
        _.transform(addAttachStatus)
          .transform(addTypeCol)
          .genDwId("tag_dw_id", startId)
          .transformForIWH2(
            TagAssociationCols,
            TagEntityPrefix,
            0, //unnecessary field
            List(TaggedEvent),
            List(UntaggedEvent),
            groupKey,
            inactiveStatus = TagInactiveStatus
          )
    )

    List(
      tagDf.map(df => {
        DataSink(getSink(TagMutatedTransformService).head, df, controlTableUpdateOptions = Map(ProductMaxIdType -> TagKey))
      })
    )
  }

  def addTypeCol(df: DataFrame): DataFrame =
    df.withColumn(s"${TagEntityPrefix}_association_type",
      when(col("type") === "SCHOOL", lit(TagSchoolAssociationType))
        .when(col("type") === "GRADE", lit(TagGradeAssociationType))
        .otherwise(lit(TagUndefinedAssociationType))
    )

  def addAttachStatus(df: DataFrame): DataFrame = {
    df.withColumn(
      s"${TagEntityPrefix}_association_attach_status",
      when(col("eventType") === TaggedEvent, lit(TagAssociationAttachedStatusVal))
        .when(col("eventType") === UntaggedEvent, lit(TagAssociationDetachedStatusVal))
        .otherwise(lit(TagAssociationUndefinedStatusVal))
    )
  }
}

object TagDimensionTransform {

  val TagMutatedTransformService: String = "transform-tag-mutated-service"
  val ParquetTagStagingSource = "parquet-tag-mutated-staging-source"
  val TagEntityPrefix = "tag"
  val TagKey = "dim_tag"

  val uniqueIdCols: List[String] = List(
    s"${TagEntityPrefix}_association_id",
    s"${TagEntityPrefix}_id",
    s"${TagEntityPrefix}_association_type"
  )

  val TagInactiveStatus = 4

  val session: SparkSession = SparkSessionUtils.getSession(TagMutatedTransformService)
  val service: SparkBatchService = new SparkBatchService(TagMutatedTransformService, session)

  val TagAssociationCols: Map[String, String] = Map(
    "tag_dw_id" -> "tag_dw_id",
    "id" -> "tag_association_id",
    "tagId" -> "tag_id",
    "name" -> "tag_name",
    "type" -> "tag_type",
    "tag_association_type" -> "tag_association_type",
    "tag_status" -> "tag_status",
    "tag_association_attach_status" -> "tag_association_attach_status",
    "occurredOn" -> "occurredOn"
  )

  def main(args: Array[String]): Unit = {
    val transformer = new TagDimensionTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }
}