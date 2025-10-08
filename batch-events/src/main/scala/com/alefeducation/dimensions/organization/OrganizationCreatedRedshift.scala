package com.alefeducation.dimensions.organization

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.organization.OrganizationCreatedDelta.OrganizationTransformedCreatedSource
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.BatchTransformerUtility._


object OrganizationCreatedRedshift {
  import OrganizationTransform._

  val OrganizationCreatedRedshiftService = "redshift-organization-create"
  val OrganizationRedshiftSink = "redshift-organization-sink"

  private val session = SparkSessionUtils.getSession(OrganizationCreatedRedshiftService)
  val service = new SparkBatchService(OrganizationCreatedRedshiftService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val createdSource = service.readOptional(OrganizationTransformedCreatedSource, session)
      createdSource.map(_.toRedshiftInsertSink(OrganizationRedshiftSink, OrganizationCreatedEvent))
    }
  }

}
