package com.alefeducation.dimensions.class_.redshift

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.class_.redshift.ClassDwIdMappingRedshift.{classDwIdMappingSinkName, classDwIdMappingSourceName}
import com.alefeducation.dimensions.class_.transform.ClassDwIdMappingTransform.classDwIdMappingTransformSink
import com.alefeducation.util.Helpers.RedshiftDwIdMappingsStageSink
import com.alefeducation.util.redshift.options.UpsertOptions
import com.alefeducation.util.{BatchTransformerUtility, SparkSessionUtils}
import org.apache.spark.sql.SparkSession

class ClassDwIdMappingRedshift(val session: SparkSession, val service: SparkBatchService) {

  import BatchTransformerUtility._
  def transform(): Option[Sink] = {
    val classDwIdMapping = service.readUniqueOptional(
      classDwIdMappingSourceName,
      session,
      uniqueColNames = Seq("id", "entity_type")
    )

    classDwIdMapping.map(
      _.toRedshiftUpsertSink(
        classDwIdMappingSinkName,
        UpsertOptions.RelDwIdMappings.targetTableName,
        matchConditions = UpsertOptions.RelDwIdMappings.matchConditions("class"),
        columnsToUpdate = UpsertOptions.RelDwIdMappings.columnsToUpdate,
        columnsToInsert = UpsertOptions.RelDwIdMappings.columnsToInsert,
        isStaging = true
      )
    )
  }
}
object ClassDwIdMappingRedshift {
  val classDwIdMappingSourceName: String = classDwIdMappingTransformSink
  val classDwIdMappingSinkName: String = RedshiftDwIdMappingsStageSink
  private val classDwIdMappingServiceName: String = "redshift-class-dw-id-mapping"

  private val classDwIdMappingSession: SparkSession = SparkSessionUtils.getSession(classDwIdMappingServiceName)
  private val classDwIdMappingService = new SparkBatchService(classDwIdMappingServiceName, classDwIdMappingSession)

  def main(args: Array[String]): Unit = {
    val transformer = new ClassDwIdMappingRedshift(classDwIdMappingSession, classDwIdMappingService)
    classDwIdMappingService.run(transformer.transform())
  }

}
