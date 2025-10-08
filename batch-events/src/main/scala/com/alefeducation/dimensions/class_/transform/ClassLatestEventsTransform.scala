package com.alefeducation.dimensions.class_.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.class_.transform.ClassLatestEventsTransform.classLatestEventsTransformSink
import com.alefeducation.schema.Schema.schema
import com.alefeducation.schema.course.{ClassMaterial, ClassSettings}
import com.alefeducation.service.DataSink
import com.alefeducation.util.Helpers.{ParquetClassDeletedSource, ParquetClassModifiedSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{ArrayType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ClassLatestEventsTransform(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._
  def transform(): Option[Sink] = {
    val classModifiedSource = service.readOptional(ParquetClassModifiedSource, session, extraProps = List(("mergeSchema", "true")))
      .map(_.addColIfNotExists("academicCalendarId", StringType))
      .map(_.cache())

    val classDeletedSource = service.readOptional(ParquetClassDeletedSource, session)

    val classLatestEvents = getLatestEvents(classModifiedSource, classDeletedSource)

    classLatestEvents.map(DataSink(classLatestEventsTransformSink, _))
  }

  private def getLatestEvents(classModifiedSource: Option[DataFrame], classDeletedSource: Option[DataFrame]): Option[DataFrame] = {

    val classDeleted = classDeletedSource.map(
      _.withColumn("title", lit(null).cast(StringType))
        .withColumn("schoolId", lit(null).cast(StringType))
        .withColumn("gradeId", lit(null).cast(StringType))
        .withColumn("sectionId", lit(null).cast(StringType))
        .withColumn("academicYearId", lit(null).cast(StringType))
        .withColumn("academicCalendarId", lit(null).cast(StringType))
        .withColumn("subjectCategory", lit(null).cast(StringType))
        .withColumn("material", lit(null).cast(schema[ClassMaterial]))
        .withColumn("settings", lit(null).cast(schema[ClassSettings]))
        .withColumn("teachers", lit(null).cast(ArrayType(StringType)))
        .withColumn("status", lit(null).cast(StringType))
        .withColumn("sourceId", lit(null).cast(StringType))
        .withColumn("categoryId", lit(null).cast(StringType))
        .withColumn("organisationGlobal", lit(null).cast(StringType)))

    classModifiedSource
      .unionOptionalByName(classDeleted)
      .map(df => getLatestRecords(df, "classId", "occurredOn"))
  }
}
object ClassLatestEventsTransform {
  val classLatestEventsTransformService = "transform-class-latest-events"
  val classLatestEventsTransformSink = "transformed-class-latest-events-sink"

  private val classLatestEventsSession: SparkSession = SparkSessionUtils.getSession(classLatestEventsTransformService)
  private val classLatestEventsService = new SparkBatchService(classLatestEventsTransformService, classLatestEventsSession)

  def main(args: Array[String]): Unit = {
    val transformer = new ClassLatestEventsTransform(classLatestEventsSession, classLatestEventsService)
    classLatestEventsService.run(transformer.transform())
  }

}
