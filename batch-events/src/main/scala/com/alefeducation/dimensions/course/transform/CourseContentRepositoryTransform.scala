package com.alefeducation.dimensions.course.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.course.transform.CourseContentRepositoryTransform._
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants._
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode, explode_outer}


class CourseContentRepositoryTransform(val session: SparkSession, val service: SparkBatchService) {

  def transform(): List[Sink] = {
    val coursePublished: Option[DataFrame] =
      service.readOptional(CoursePublishedParquetSource, session, extraProps = List(("mergeSchema", "true")))
    val contentRepoMaterialAttachedDf = service.readOptional(ContentRepositoryMaterialAttachSource, session, extraProps = List(("mergeSchema", "true")))
      .map(_.withColumn("material", explode(col("addedMaterials")))
        .withColumnRenamed("id", "ccr_content_repository")
        .withColumn("id", col("material.id"))
        .withColumn("courseType", col("material.type"))
        .select("id", "ccr_content_repository", "courseType", "occurredOn", "eventType"))

    val contentRepoMaterialDetachedDf = service.readOptional(ContentRepositoryMaterialDetachSource, session, extraProps = List(("mergeSchema", "true")))
      .map(_.withColumn("material", explode(col("removedMaterials")))
        .withColumnRenamed("id", "ccr_content_repository")
        .withColumn("id", col("material.id"))
        .withColumn("courseType", col("material.type"))
        .select("id", "ccr_content_repository", "courseType", "occurredOn", "eventType"))

    val startId = service.getStartIdUpdateStatus(CourseContentRepositoryKey)

    val courseContentRepositoryData = coursePublished.map(
      _.withColumn("ccr_content_repository", explode_outer(col("subOrganisations")))
        .select("id", "ccr_content_repository", "courseType", "occurredOn", "eventType")
    )
    val allCourseContentRepos = courseContentRepositoryData.unionOptionalByName(contentRepoMaterialAttachedDf).unionOptionalByName(contentRepoMaterialDetachedDf)
    val courseCreated = allCourseContentRepos.flatMap(
      _.transformForIWH2(
        CourseContentRepositoryCols,
        CourseContentRepositoryEntityPrefix,
        0,
        List(CoursePublishedEvent, ContentRepositoryMaterialAttachedEvent),
        List(ContentRepositoryMaterialDetachedEvent),
        List("id", "ccr_content_repository"),
        inactiveStatus = CourseInactiveStatusVal
      ).genDwId("ccr_dw_id", startId)
        .checkEmptyDf
    )
    courseCreated.map(DataSink(CourseContentRepositoryTransformedSink, _, controlTableUpdateOptions = Map(ProductMaxIdType -> CourseContentRepositoryKey))).toList
  }
}

object CourseContentRepositoryTransform {
  val CourseContentRepositoryKey = "dim_course_content_repository_association"
  val CoursePublishedTransformService = "course-content-repository-transform"
  val ContentRepositoryMaterialAttachSource = "ccl-content-repository-material-attached-parquet"
  val ContentRepositoryMaterialDetachSource = "ccl-content-repository-material-detached-parquet"
  val CoursePublishedParquetSource = "parquet-course-published-source"
  val CourseContentRepositoryEntityPrefix = "ccr"
  val CourseContentRepositoryTransformedSink = "parquet-course-content-repository-transformed-sink"
  val CourseContentRepositoryCols: Map[String, String] = Map[String, String](
    "id" -> "ccr_course_id",
    "ccr_content_repository" -> "ccr_repository_id",
    "ccr_status" -> "ccr_status",
    "courseType" -> "ccr_course_type",
    "occurredOn" -> "occurredOn",
    "ccr_attach_status" -> "ccr_attach_status"
  )

  val session: SparkSession = SparkSessionUtils.getSession(CoursePublishedTransformService)
  val service = new SparkBatchService(CoursePublishedTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new CourseContentRepositoryTransform(session, service)
    service.runAll(transformer.transform())
  }
}
