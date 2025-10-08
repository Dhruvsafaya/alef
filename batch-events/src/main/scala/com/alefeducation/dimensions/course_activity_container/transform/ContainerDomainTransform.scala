package com.alefeducation.dimensions.course_activity_container.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.course_activity_container.transform.ContainerDomainTransform.{ContainerDomainKey, TablePrefix, mappings}
import com.alefeducation.dimensions.course_activity_container.transform.ContainerMutatedTransform.{ContainerAddedEvent, ContainerPublishedEvent, ContainerUpdatedEvent}
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.{getSink, getSource}
import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers.Deleted
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.types.BooleanType


class ContainerDomainTransform(val session: SparkSession, val service: SparkBatchService)  {
  val requiredCols: List[String] = List(
    "eventType",
    "id",
    "courseId",
    "occurredOn",
    "sequences"
  )

  def transform(sourceNames: List[String], sinkName: String): Option[Sink] = {
    import session.implicits._

    val sourceFiles = sourceNames.map(service.readOptional(_, session).map(_
      .select(requiredCols.head, requiredCols.tail:_*)
      .addColIfNotExists("isAccelerated", BooleanType)
      .withColumn("sequence", F.explode_outer($"sequences"))
      .select("eventType",
        "id",
        "courseId",
        "occurredOn",
        "sequence")
    ))
    val transformed = sourceFiles.tail.foldLeft(sourceFiles.head)(_ unionOptionalByName  _)

    val startId = service.getStartIdUpdateStatus(ContainerDomainKey)

    val mutatedIWH: Option[DataFrame] = transformed.flatMap(
      _.transformForIWH2(
        mappings,
        TablePrefix,
        associationType = 0, //unnecessary field
        attachedEvents = List(ContainerPublishedEvent, ContainerAddedEvent, ContainerUpdatedEvent),
        Nil,
        groupKey = List("id"),
        associationDetachedStatusVal = Deleted,
        inactiveStatus = Deleted
      ).genDwId("cacd_dw_id", startId)
        .checkEmptyDf
    )
    mutatedIWH.map(DataSink(sinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> ContainerDomainKey)))
  }
}

object ContainerDomainTransform {
  val ContainerDomainKey = "dim_course_activity_container_domain"
  val ContainerDomainTransformService = "container-domain-transform"
  val TablePrefix = "cacd"
  val ContainerDomainTransformedSink = "container-domain-transform-sink"

  val mappings: Map[String, String] = Map[String, String](
    "id" -> "cacd_container_id",
    "courseId" -> "cacd_course_id",
    "sequence.domain" -> "cacd_domain",
    "sequence.sequence" -> "cacd_sequence",
    "occurredOn" -> "occurredOn",
    "cacd_status" -> "cacd_status"
  )

  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSessionUtils.getSession(ContainerDomainTransformService)
    val service = new SparkBatchService(ContainerDomainTransformService, session)
    val t = new ContainerDomainTransform(session, service)
    val sourceNames = getSource(ContainerDomainTransformService)
    val sinkName = getSink(ContainerDomainTransformService).head
    service.run(t.transform(sourceNames, sinkName))
  }
}
