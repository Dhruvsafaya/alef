package com.alefeducation.dimensions.organization

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.organization.OrganizationTransform.OrganizationMutatedParquetSource
import com.alefeducation.util.{ParquetBatchWriter, SparkSessionUtils}

object OrganizationParquet {
  val OrganizationParquetService = "parquet-organization"
  val OrganizationMutatedParquetSink =  "parquet-ccl-organization-mutated-sink"
  val session = SparkSessionUtils.getSession(OrganizationParquetService)
  val service = new SparkBatchService(OrganizationParquetService, session)
  def main(args: Array[String]): Unit = {
    val writer = new ParquetBatchWriter(session, service, OrganizationMutatedParquetSource, OrganizationMutatedParquetSink)
    service.run(writer.write())
  }
}
