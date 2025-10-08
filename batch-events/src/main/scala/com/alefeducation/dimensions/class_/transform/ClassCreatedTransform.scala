package com.alefeducation.dimensions.class_.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.class_.transform.ClassCreatedTransform.{classCreatedTransformSink, classKey, castToLong}
import com.alefeducation.dimensions.class_.transform.ClassLatestEventsTransform.classLatestEventsTransformSink
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants.ClassCreatedEvent
import com.alefeducation.util.Helpers.{ClassDimensionCols, ClassEntity, NULL}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}
class ClassCreatedTransform(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._
  import session.implicits._

  def transform(): Option[Sink] = {
    val classLatestEvents = service.readOptional(classLatestEventsTransformSink, session)

    val startId = service.getStartIdUpdateStatus(classKey)

    val classCreatedEvents = classLatestEvents.flatMap(_.filter($"eventType" === ClassCreatedEvent).checkEmptyDf)
    val classCreated = classCreatedEvents.map(
      _.transformForInsertDim(ClassDimensionCols, ClassEntity, ids = List("classId"))
        .withColumn("class_active_until", lit(NULL).cast(TimestampType))
        .transform(castToLong)
        .genDwId("rel_class_dw_id", startId)
    )

    classCreated.map(
      DataSink(classCreatedTransformSink, _, controlTableUpdateOptions = Map(ProductMaxIdType -> classKey))
    )
  }
}
object ClassCreatedTransform {
  val classCreatedTransformService = "transform-class-created"
  val classCreatedTransformSink = "transformed-class-created-sink"

  val classKey = "dim_class"

  private val classCreatedSession: SparkSession = SparkSessionUtils.getSession(classCreatedTransformService)
  private val classCreatedService = new SparkBatchService(classCreatedTransformService, classCreatedSession)

  def main(args: Array[String]): Unit = {
    val transformer = new ClassCreatedTransform(classCreatedSession, classCreatedService)
    classCreatedService.run(transformer.transform())
  }

  def castToLong(df: DataFrame): DataFrame =
    df.withColumn("class_curriculum_id", col("class_curriculum_id").cast(LongType))
      .withColumn("class_curriculum_grade_id", col("class_curriculum_grade_id").cast(LongType))
      .withColumn("class_curriculum_subject_id", col("class_curriculum_subject_id").cast(LongType))

}
