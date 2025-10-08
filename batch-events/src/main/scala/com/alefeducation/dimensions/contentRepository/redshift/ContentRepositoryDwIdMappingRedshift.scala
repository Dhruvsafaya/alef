package com.alefeducation.dimensions.contentRepository.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.redshift.options.UpsertOptions


object ContentRepositoryDwIdMappingRedshift {

  val RedshiftContentRepositoryDwIdMapping =  "redshift-content-repository-dw-id-mapping"
  val ContentRepositoryDwIdMappingSource = "content-repository-dw-id-mapping-source"
  val ContentRepositoryDwIdMappingRedshiftSink = "content-repository-dw-id-mapping-redshift-sink"

  private val session = SparkSessionUtils.getSession(RedshiftContentRepositoryDwIdMapping)
  val service = new SparkBatchService(RedshiftContentRepositoryDwIdMapping, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val contentRepositoryIdSource = service.readUniqueOptional(ContentRepositoryDwIdMappingSource, session, uniqueColNames = List("id", "entity_type"))
      contentRepositoryIdSource.map(
        _.toRedshiftUpsertSink(
          ContentRepositoryDwIdMappingRedshiftSink,
          UpsertOptions.RelDwIdMappings.targetTableName,
          matchConditions = UpsertOptions.RelDwIdMappings.matchConditions("content-repository"),
          columnsToUpdate = UpsertOptions.RelDwIdMappings.columnsToUpdate,
          columnsToInsert = UpsertOptions.RelDwIdMappings.columnsToInsert,
          isStaging = true
        )
      )
    }
  }
}
