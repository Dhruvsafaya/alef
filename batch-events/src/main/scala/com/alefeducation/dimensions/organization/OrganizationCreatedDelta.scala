package com.alefeducation.dimensions.organization

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.SparkSessionUtils

object OrganizationCreatedDelta {

  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  val OrganizationDeltaCreatedService = "delta-organization-created"
  val OrganizationTransformedCreatedSource = "organization-transformed-created-source"
  val OrganizationDeltaSink = "delta-organization-sink"

  private val session = SparkSessionUtils.getSession(OrganizationDeltaCreatedService)
  val service = new SparkBatchService(OrganizationDeltaCreatedService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val createdSource = service.readOptional(OrganizationTransformedCreatedSource, session)
      createdSource.flatMap(_.toCreate().map(_.toSink(OrganizationDeltaSink)))
    }
  }
}
