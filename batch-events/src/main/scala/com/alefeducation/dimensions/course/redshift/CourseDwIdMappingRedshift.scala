package com.alefeducation.dimensions.course.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.course.transform.CourseDwIdMappingTransform.CourseDwIdMappingSink
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.util.redshift.options.UpsertOptions
import com.alefeducation.util.BatchTransformerUtility._


object  CourseDwIdMappingRedshift {
  val CourseDwIdMappingService = "redshift-course-dw-id-mapping-service"

  val CourseDwIdMappingTransformedSource: String = CourseDwIdMappingSink
  val CourseDwIdMappingRedshiftSink: String = "redshift-course-dw-id-mapping-sink"

  private val session = SparkSessionUtils.getSession(CourseDwIdMappingService)
  val service = new SparkBatchService(CourseDwIdMappingService, session)

  def main(args: Array[String]): Unit = {
    service.run {
      val CourseDwIdMappingCreatedSource = service.readUniqueOptional(CourseDwIdMappingTransformedSource, session, uniqueColNames = List("id", "entity_type"))
      CourseDwIdMappingCreatedSource.map(
        _.toRedshiftUpsertSink(
          CourseDwIdMappingRedshiftSink,
          UpsertOptions.RelDwIdMappings.targetTableName,
          matchConditions = UpsertOptions.RelDwIdMappings.matchConditions("course"),
          columnsToUpdate = UpsertOptions.RelDwIdMappings.columnsToUpdate,
          columnsToInsert = UpsertOptions.RelDwIdMappings.columnsToInsert,
          isStaging = true
        )
      )
    }
  }
}
