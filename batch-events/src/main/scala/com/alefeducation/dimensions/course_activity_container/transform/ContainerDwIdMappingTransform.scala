package com.alefeducation.dimensions.course_activity_container.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.course_activity_container.transform.ContainerDwIdMappingTransform.{ContainerAddedParquetSource, ContainerDwIdMappingSink, ContainerPublishedParquetSource}
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}


class ContainerDwIdMappingTransform(val session: SparkSession, val service: SparkBatchService) {

  def transform(): List[Option[Sink]] = {

    val containerPublished: Option[DataFrame] = service.readOptional(
      ContainerPublishedParquetSource, session
    ).map(_.select("id", "occurredOn"))
    val containerAdded: Option[DataFrame] = service.readOptional(
      ContainerAddedParquetSource, session
    ).map(_.select("id", "occurredOn"))

    val container: Option[DataFrame] = containerPublished.unionOptionalByNameWithEmptyCheck(containerAdded)

    val dwIdMapping: Option[DataFrame] = container.map(
      _.transformForInsertDwIdMapping("entity", "course_activity_container")
    )

    List(
      dwIdMapping.map(DataSink(ContainerDwIdMappingSink, _)),
    )
  }
}

object ContainerDwIdMappingTransform {
  val ContainerDwIdMappingTransformService = "container-dw-id-mapping-transform"

  val ContainerPublishedParquetSource = "parquet-container-published-source"
  val ContainerAddedParquetSource = "parquet-container-added-source"

  val ContainerDwIdMappingSink = "parquet-container-dw-id-mapping-transformed-sink"

  val session: SparkSession = SparkSessionUtils.getSession(ContainerDwIdMappingTransformService)
  val service = new SparkBatchService(ContainerDwIdMappingTransformService, session)

  def main(args: Array[String]): Unit = {
    service.runAll {
      val transformer = new ContainerDwIdMappingTransform(session, service)
      transformer.transform().flatten
    }
  }
}



