package com.alefeducation.dimensions.organization

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.organization.OrganizationTransform.OrganizationEntityPrefix
import com.alefeducation.dimensions.organization.OrganizationUpdatedRedshift.OrganizationTransformedUpdatedSource
import com.alefeducation.util.SparkSessionUtils

object OrganizationUpdatedDelta {
  import com.alefeducation.bigdata.batch.delta.Delta.Transformations._

  val OrganizationDeltaUpdatedService = "delta-organization-updated"
  val OrganizationDeltaSink = "delta-organization-sink"

  private val session = SparkSessionUtils.getSession(OrganizationDeltaUpdatedService)
  val service = new SparkBatchService(OrganizationDeltaUpdatedService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val createdSource = service.readOptional(OrganizationTransformedUpdatedSource, session)
      createdSource.flatMap(_.toUpdate().map(_.toSink(OrganizationDeltaSink, OrganizationEntityPrefix, "organization_id")))
    }
  }
}
