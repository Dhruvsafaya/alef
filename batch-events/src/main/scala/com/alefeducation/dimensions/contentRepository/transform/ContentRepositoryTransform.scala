package com.alefeducation.dimensions.contentRepository.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.contentRepository.transform.ContentRepositoryTransform._
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

class ContentRepositoryTransform(val session: SparkSession, val service: SparkBatchService) {

  import session.implicits._
  def transform(): List[Option[Sink]] = {
    val contentRepositoryMutatedDF: Option[DataFrame] = service.readOptional(ContentRepositoryMutatedParquetSource, session)

    val dwIdMappingDf  = contentRepositoryMutatedDF.flatMap(
      _.select($"id", $"occurredOn")
        .transformForInsertDwIdMapping("entity", "content-repository")
        .checkEmptyDf).map(DataSink(ContentRepositoryDwIdMappingSink, _))

    val contentRepositoryCreated = contentRepositoryMutatedDF.map(
      _.transformForIWH2(ContentRepositoryCols,
          ContentRepositoryEntityPrefix,
          1,
          List(ContentRepositoryCreatedEvent),
          Nil,
          List("id")
        )).map(DataSink(ContentRepositoryTransformedCreatedSink, _))

    List(contentRepositoryCreated, dwIdMappingDf)
  }
}

object ContentRepositoryTransform {
  val ContentRepositoryMutatedParquetSource = "parquet-content-repository-mutated-source"
  val ContentRepositoryCreatedEvent = "ContentRepositoryCreatedEvent"
  val ContentRepositoryCols: Map[String, String] = Map[String, String](
    "id" -> "content_repository_id",
    "name" -> "content_repository_name",
    "organisation" -> "content_repository_organisation_owner",
    "occurredOn" -> "occurredOn",
    "content_repository_status" -> "content_repository_status",
  )
  val ContentRepositoryEntityPrefix = "content_repository"
  val ContentRepositoryTransformedCreatedSink = "content-repository-transformed-created-sink"
  val ContentRepositoryDwIdMappingSink = "content-repository-dw-id-mapping-sink"

  val ContentRepositoryTransformService = "content-repository-transform"

  val session: SparkSession = SparkSessionUtils.getSession(ContentRepositoryTransformService)
  val service = new SparkBatchService(ContentRepositoryTransformService, session)

  def main(args: Array[String]): Unit = {
    service.runAll {
      val transformer = new ContentRepositoryTransform(session, service)
      transformer.transform().flatten
    }
  }
}


