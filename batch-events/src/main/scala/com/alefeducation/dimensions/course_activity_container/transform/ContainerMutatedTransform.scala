package com.alefeducation.dimensions.course_activity_container.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.course_activity_container.transform.ContainerMutatedTransform._
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers.Deleted
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}


class ContainerMutatedTransform(val session: SparkSession, val service: SparkBatchService) {

  def readCommonContainer(parquetSource: String,
                             session: SparkSession,
                             service: SparkBatchService): Option[DataFrame] = {
    import session.implicits._

    val container: Option[DataFrame] = service.readOptional(parquetSource, session)

    container.map(
      _.withColumn("sequence", F.explode_outer($"sequences"))
        .addColIfNotExists("isAccelerated", BooleanType)
        .selectColumnsWithMapping(ContainerCols)
    )
  }

  def transform(): List[Option[Sink]] = {
    val containerPublished: Option[DataFrame] = readCommonContainer(ContainerPublishedParquetSource, session, service)
    val containerAdded: Option[DataFrame] = readCommonContainer(ContainerAddedParquetSource, session, service)
    val containerUpdated: Option[DataFrame] = readCommonContainer(ContainerUpdatedParquetSource, session, service)

    val containerDeleted: Option[DataFrame] = service.readOptional(ContainerDeletedParquetSource, session)
      .map(
        _.selectColumnsWithMapping(ContainerDeletedCols)
          .addColIfNotExists("course_activity_container_title", StringType)
          .addColIfNotExists("course_activity_container_pacing", StringType)
          .addColIfNotExists("course_activity_container_domain", StringType)
          .addColIfNotExists("course_activity_container_sequence", IntegerType)
          .addColIfNotExists("course_activity_container_longname", StringType)
          .addColIfNotExists("course_activity_container_description", StringType)
          .addColIfNotExists("course_activity_container_metadata", metadataSchema)
          .addColIfNotExists("course_activity_container_is_accelerated", BooleanType)
      )

    val mutated = containerPublished
      .unionOptionalByNameWithEmptyCheck(containerAdded)
      .unionOptionalByNameWithEmptyCheck(containerUpdated)
      .unionOptionalByNameWithEmptyCheck(containerDeleted)

    val startId = service.getStartIdUpdateStatus(ContainerKey)

    val mutatedIWH: Option[DataFrame] = mutated.flatMap(
      _.transformForIWH2(
        ContainerColMapping,
        ContainerEntityPrefix,
        associationType = 0, //unnecessary field
        attachedEvents = List(ContainerPublishedEvent, ContainerAddedEvent, ContainerUpdatedEvent),
        detachedEvents = List(ContainerDeletedEvent),
        groupKey = List("course_activity_container_id"),
        associationDetachedStatusVal = Deleted,
        inactiveStatus = Deleted
      ).genDwId("rel_course_activity_container_dw_id", startId)
        .checkEmptyDf
    )

    List(
      mutatedIWH.map(DataSink(ContainerTransformedSink, _, controlTableUpdateOptions = Map(ProductMaxIdType -> ContainerKey)))
    )
  }
}

object ContainerMutatedTransform {
  val ContainerKey = "dim_course_activity_container"
  val ContainerTransformService = "container-mutated-transform"

  val ContainerPublishedParquetSource = "parquet-container-published-source"
  val ContainerAddedParquetSource = "parquet-container-added-source"
  val ContainerUpdatedParquetSource = "parquet-container-updated-source"
  val ContainerDeletedParquetSource = "parquet-container-deleted-source"

    val ContainerTransformedSink = "parquet-container-mutated-transformed-sink"

  val ContainerEntityPrefix = "course_activity_container"

  val ContainerPublishedEvent = "ContainerPublishedWithCourseEvent"
  val ContainerAddedEvent = "ContainerAddedInCourseEvent"
  val ContainerUpdatedEvent = "ContainerUpdatedInCourseEvent"
  val ContainerDeletedEvent = "ContainerDeletedFromCourseEvent"

  val ContainerDeletedCols: Map[String, String] = Map[String, String](
    "id" -> "course_activity_container_id",
    "courseId" -> "course_activity_container_course_id",
    "index" -> "course_activity_container_index",
    "courseVersion" -> "course_activity_container_course_version",
    "occurredOn" -> "occurredOn",
    "eventType" -> "eventType"
  )

  val ContainerCols: Map[String, String] = Map[String, String](
    "id" -> "course_activity_container_id",
    "courseId" -> "course_activity_container_course_id",
    "index" -> "course_activity_container_index",
    "title" -> "course_activity_container_title",
    "settings.pacing" -> "course_activity_container_pacing",
    "sequence.domain" -> "course_activity_container_domain",
    "sequence.sequence" -> "course_activity_container_sequence",
    "courseVersion" -> "course_activity_container_course_version",
    "longName" -> "course_activity_container_longname",
    "description" -> "course_activity_container_description",
    "metadata" -> "course_activity_container_metadata",
    "occurredOn" -> "occurredOn",
    "eventType" -> "eventType",
    "isAccelerated" -> "course_activity_container_is_accelerated"
  )

  val ContainerColMapping: Map[String, String] = Map[String, String](
    "course_activity_container_id" -> "course_activity_container_id",
    "course_activity_container_course_id" -> "course_activity_container_course_id",
    "course_activity_container_index" -> "course_activity_container_index",
    "course_activity_container_title" -> "course_activity_container_title",
    "course_activity_container_pacing" -> "course_activity_container_pacing",
    "course_activity_container_domain" -> "course_activity_container_domain",
    "course_activity_container_sequence" -> "course_activity_container_sequence",
    "course_activity_container_sequence" -> "course_activity_container_sequence",
    "course_activity_container_status" -> "course_activity_container_status",
    "course_activity_container_attach_status" -> "course_activity_container_attach_status",
    "course_activity_container_course_version" -> "course_activity_container_course_version",
    "course_activity_container_longname" -> "course_activity_container_longname",
    "course_activity_container_description" -> "course_activity_container_description",
    "course_activity_container_metadata" -> "course_activity_container_metadata",
    "occurredOn" -> "occurredOn",
    "course_activity_container_is_accelerated" -> "course_activity_container_is_accelerated"
  )

  val metadataSchema: StructType = StructType(
    Seq(
      StructField("tags",
        ArrayType(
          StructType(
            Seq(
              StructField("key", StringType),
              StructField("values", ArrayType(StringType)),
              StructField("attributes", ArrayType(StructType(
                Seq(
                  StructField("value", StringType),
                  StructField("color", StringType),
                  StructField("translation", StringType)
                )
              ))),
              StructField("type", StringType)
            )
          )
        )
      )
    )
  )

  val session: SparkSession = SparkSessionUtils.getSession(ContainerTransformService)
  val service = new SparkBatchService(ContainerTransformService, session)

  def main(args: Array[String]): Unit = {
    service.runAll {
      val transformer = new ContainerMutatedTransform(session, service)
      transformer.transform().flatten
    }
  }
}


