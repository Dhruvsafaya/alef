package com.alefeducation.dimensions.class_.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.class_.transform.ClassDwIdMappingTransform.classDwIdMappingTransformSink
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants.ClassCreatedEvent
import com.alefeducation.util.Helpers.ParquetClassModifiedSource
import com.alefeducation.util.{BatchTransformerUtility, SparkSessionUtils}
import org.apache.spark.sql.SparkSession

class ClassDwIdMappingTransform(val session: SparkSession, val service: SparkBatchService) {

  import BatchTransformerUtility._
  import session.implicits._

  def transform(): Option[Sink] = {
    val classModifiedSource = service.readOptional(ParquetClassModifiedSource, session).map(_.cache())

    val classDwIdMappingSink = classModifiedSource.map(
      _.filter($"eventType" === ClassCreatedEvent)
        .select($"classId".as("id"), $"occurredOn")
        .transformForInsertDwIdMapping("entity", "class")
    )
    classDwIdMappingSink.map(DataSink(classDwIdMappingTransformSink, _))
  }
}
object ClassDwIdMappingTransform {
  val classDwIdMappingTransformService = "transform-dw-id-mapping"
  val classDwIdMappingTransformSink = "transformed-class-dw-id-mapping-sink"

  private val classDwIdMappingSession: SparkSession = SparkSessionUtils.getSession(classDwIdMappingTransformService)
  private val classDwIdMappingService = new SparkBatchService(classDwIdMappingTransformService, classDwIdMappingSession)

  def main(args: Array[String]): Unit = {
    val transformer = new ClassDwIdMappingTransform(classDwIdMappingSession, classDwIdMappingService)
    classDwIdMappingService.run(transformer.transform())
  }

}
