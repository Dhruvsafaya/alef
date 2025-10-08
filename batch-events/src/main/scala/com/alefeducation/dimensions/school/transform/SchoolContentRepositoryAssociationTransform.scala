package com.alefeducation.dimensions.school.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.school.transform.SchoolContentRepositoryAssociationTransform.{Key, SchoolContentRepositoryAssEntity, SchoolContentRepositoryAssociationCols}
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, explode_outer}

class SchoolContentRepositoryAssociationTransform(val session: SparkSession, val service: SparkBatchService, val serviceName: String) {

  val uniqueKey: List[String] = List("uuid")

  val orderBy: String = "occurredOn"

  def transform(): Option[Sink] = {
    val sourceName = getSource(serviceName).head
    val sinkName = getSink(serviceName).head

    val source = service.readOptional(sourceName, session)

    val startId = service.getStartIdUpdateStatus(Key)

    val transformed = source.map(
      _.withColumn("contentRepositoryId", explode_outer(col("contentRepositoryIds")))
        .transformForSCDTypeII(
          SchoolContentRepositoryAssociationCols,
          SchoolContentRepositoryAssEntity,
          uniqueKey,
          orderBy
        )
        .genDwId(s"${SchoolContentRepositoryAssEntity}_dw_id", startId)
    )

    transformed.map(DataSink(sinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> Key)))
  }
}


object SchoolContentRepositoryAssociationTransform {
  val SchoolContentRepositoryAssociationService = "transform-school-content-repository-association"

  val SchoolContentRepositoryAssEntity = "scra"
  val Key = "dim_school_content_repository_association"

  val SchoolContentRepositoryAssociationCols: Map[String, String] = Map(
    "uuid" -> s"${SchoolContentRepositoryAssEntity}_school_id",
    "contentRepositoryId" -> s"${SchoolContentRepositoryAssEntity}_content_repository_id",
    s"${SchoolContentRepositoryAssEntity}_status" -> s"${SchoolContentRepositoryAssEntity}_status",
    s"${SchoolContentRepositoryAssEntity}_active_until" -> s"${SchoolContentRepositoryAssEntity}_active_until",
    "occurredOn" -> "occurredOn"
  );
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSessionUtils.getSession(SchoolContentRepositoryAssociationService)
    val service = new SparkBatchService(SchoolContentRepositoryAssociationService, session)

    val transform = new SchoolContentRepositoryAssociationTransform(session, service, SchoolContentRepositoryAssociationService)
    service.run(transform.transform())
  }
}