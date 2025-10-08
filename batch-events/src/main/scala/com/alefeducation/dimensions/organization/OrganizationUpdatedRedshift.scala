package com.alefeducation.dimensions.organization


import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.BatchTransformerUtility._


object OrganizationUpdatedRedshift {
  import OrganizationTransform._

  val OrganizationUpdateRedshiftService = "redshift-organization-update"
  val OrganizationTransformedUpdatedSource = "organization-transformed-updated-source"
  val OrganizationRedshiftSink = "redshift-organization-sink"

  private val session = SparkSessionUtils.getSession(OrganizationUpdateRedshiftService)
  val service = new SparkBatchService(OrganizationUpdateRedshiftService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val updatedSource = service.readOptional(OrganizationTransformedUpdatedSource, session)
      updatedSource.map(_.toRedshiftUpdateSink(OrganizationRedshiftSink, OrganizationUpdatedEvent, OrganizationEntityPrefix, List("organization_id")))
    }
  }
}
