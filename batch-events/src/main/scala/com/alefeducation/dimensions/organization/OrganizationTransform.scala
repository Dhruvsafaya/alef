package com.alefeducation.dimensions.organization

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.service.DataSink
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Helpers.{ActiveEnabled, Disabled}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._

class OrganizationTransform(val session: SparkSession, val service: SparkBatchService) {

  import OrganizationTransform._
  import session.implicits._

  def transform(): List[Option[Sink]] = {
    val organizationMutatedDF: Option[DataFrame] = service.readOptional(OrganizationMutatedParquetSource, session)

    val organizationCreated = organizationMutatedDF.flatMap(
      _.filter($"eventType" === OrganizationCreatedEvent)
        .transformForInsertDim(OrganizationCols, OrganizationEntityPrefix, ids = List("id"))
        .withColumn("organization_status",
          when(col("organization_status") === true, lit(ActiveEnabled)).otherwise(lit(Disabled)))
        .checkEmptyDf
    ).map(DataSink(OrganizationTransformedCreatedSink, _))

    val organizationUpdated = organizationMutatedDF.flatMap(
      _.filter($"eventType" === OrganizationUpdatedEvent)
        .transformForUpdateDim(OrganizationCols, OrganizationEntityPrefix, ids = List("id"))
        .withColumn("organization_status",
          when(col("organization_status") === true, lit(ActiveEnabled)).otherwise(lit(Disabled)))
        .checkEmptyDf
    ).map(DataSink(OrganizationTransformedUpdatedSink, _))
    List(organizationCreated, organizationUpdated)
  }
}

object OrganizationTransform {
  val OrganizationMutatedParquetSource = "parquet-ccl-organization-mutated-source"
  val OrganizationCreatedEvent = "OrganizationCreatedEvent"
  val OrganizationUpdatedEvent = "OrganizationUpdatedEvent"
  val OrganizationCols: Map[String, String] = Map[String, String](
    "id" -> "organization_id",
    "name" -> "organization_name",
    "country" -> "organization_country",
    "code" -> "organization_code",
    "isActive" -> "organization_status",
    "tenantCode" -> "organization_tenant_code",
    "createdBy" -> "organization_created_by",
    "updatedBy" -> "organization_updated_by",
    "occurredOn" -> "occurredOn"
  )
  val OrganizationEntityPrefix = "organization"
  val OrganizationTransformedCreatedSink = "organization-transformed-created-sink"
  val OrganizationTransformedUpdatedSink = "organization-transformed-updated-sink"

  val OrganizationTransformService = "organization-transform"

  val session: SparkSession = SparkSessionUtils.getSession(OrganizationTransformService)
  val service = new SparkBatchService(OrganizationTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new OrganizationTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }
}
