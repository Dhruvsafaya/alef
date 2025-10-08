package com.alefeducation.dimensions.course_activity_container.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.redshift.options.UpsertOptions


object ContainerDwIdMappingRedshift {
  val ContainerDwIdMappingService = "redshift-container-dw-id-mapping"

  val ContainerDwIdMappingTransformedSource: String = "parquet-container-dw-id-mapping-transformed-source"
  val ContainerDwIdMappingRedshiftSink: String = "redshift-course-activity-container-dw-id-sink"

  private val session = SparkSessionUtils.getSession(ContainerDwIdMappingService)
  val service = new SparkBatchService(ContainerDwIdMappingService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val PathwayDwIdMappingCreatedSource = service.readUniqueOptional(ContainerDwIdMappingTransformedSource, session, uniqueColNames = List("id", "entity_type"))
      PathwayDwIdMappingCreatedSource.map(
        _.toRedshiftUpsertSink(
          ContainerDwIdMappingRedshiftSink,
          UpsertOptions.RelDwIdMappings.targetTableName,
          matchConditions = UpsertOptions.RelDwIdMappings.matchConditions("course_activity_container"),
          columnsToUpdate = UpsertOptions.RelDwIdMappings.columnsToUpdate,
          columnsToInsert = UpsertOptions.RelDwIdMappings.columnsToInsert,
          isStaging = true
        )
      )
    }
  }

}
