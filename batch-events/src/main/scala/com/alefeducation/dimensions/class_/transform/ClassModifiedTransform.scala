package com.alefeducation.dimensions.class_.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.class_.transform.ClassCreatedTransform.{castToLong, classKey}
import com.alefeducation.dimensions.class_.transform.ClassLatestEventsTransform.classLatestEventsTransformSink
import com.alefeducation.dimensions.class_.transform.ClassModifiedTransform.classModifiedTransformSink
import com.alefeducation.models.ClassModel.DimClass
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants.{ClassDeletedEvent, ClassUpdatedEvent}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.{Resources, SparkSessionUtils}
import org.apache.spark.sql.functions.{col, to_utc_timestamp}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.util.Calendar

class ClassModifiedTransform(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._
  import session.implicits._
  def transform(): Option[Sink] = {
    val classLatestEvents = service.readOptional(classLatestEventsTransformSink, session)
    val classModifiedSource = service.readOptional(ParquetClassModifiedSource, session, extraProps = List(("mergeSchema", "true"))).map(_.cache())
    val classDeletedSource = service.readOptional(ParquetClassDeletedSource, session)

    //Updated
    val classUpdatedEvents = classLatestEvents.flatMap(_.filter($"eventType" === ClassUpdatedEvent).checkEmptyDf)
    val classUpdated = classUpdatedEvents.map(
      _.transformForSCD(ClassDimensionCols, ClassEntity, ids = List("classId"))
        .transform(castToLong)
    )

    //Deleted
    val deletedEvents = classLatestEvents.flatMap(
      _.filter($"eventType" === ClassDeletedEvent)
        .select("classId", "occurredOn", "eventType")
        .checkEmptyDf
    )
    val deletedClassRedshiftSource = classDeletedSource.map(
      deletedEvents => {
        val deletedIds = deletedEvents.select("classId").as[String].collect().toList.mkString("'", "','", "'")

        val relClass = service.readFromRedshiftQuery[DimClass](
          s"select * from ${Resources.redshiftStageSchema()}.rel_class where class_id in ($deletedIds) and class_active_until is null"
        )
        val dimClass = service.readFromRedshiftQuery[DimClass](
          s"select * from ${Resources.redshiftSchema()}.dim_class where class_id in ($deletedIds) and class_active_until is null"
        )

        relClass.unionByName(dimClass).distinct()
      }
    )
    val latestDeleteRecords = enrichDeletedEvents(deletedClassRedshiftSource, classModifiedSource, deletedEvents)
    val classDeleted = latestDeleteRecords.map(
      _.transformForSCD(ClassDeletedCols, ClassEntity, ids = List("classId"))
        .transform(castToLong)
    )

    val startId = service.getStartIdUpdateStatus(classKey)
    val modified = classUpdated.unionOptionalByName(classDeleted).map(
      _.genDwId("rel_class_dw_id", startId, orderByField="class_created_time")
    )

    modified.map(
      DataSink(classModifiedTransformSink, _, controlTableUpdateOptions = Map(ProductMaxIdType -> classKey))
    )
  }

  private def enrichDeletedEvents(deletedClassRedshiftSource: Option[DataFrame],
                                  classModifiedSource: Option[DataFrame],
                                  classDeletedEvents: Option[DataFrame]): Option[DataFrame] = {
    classDeletedEvents.flatMap(deletedEvents => {
      //Get class info from the input source
      val classEvents = classModifiedSource.map(
        _.appendStatusColumn(ClassEntity)
          .addColIfNotExists("academicCalendarId", StringType)
          .selectColumnsWithMapping(ClassDimensionCols)
          .withColumn("class_created_time", to_utc_timestamp(col("occurredOn"), Calendar.getInstance().getTimeZone.getID))
          .drop("occurredOn")
      )

      //Combine Redshift and input class info
      val combinedClassSource: Option[DataFrame] = deletedClassRedshiftSource.map(_.unionOptionalByName(classEvents))
      val latestRecords = combinedClassSource.map(getLatestRecords(_, "class_id", "class_created_time"))

      //Enrich deleted event with the combined class info
      latestRecords.map(latestRecord =>
        latestRecord.join(deletedEvents, latestRecord.col("class_id") === deletedEvents.col("classId"))
          .drop("class_id")
          .cache()
      )
    })

  }
}
object ClassModifiedTransform {
  val classModifiedTransformService = "transform-class-modified"
  val classModifiedTransformSink = "transformed-class-modified-sink"

  private val session: SparkSession = SparkSessionUtils.getSession(classModifiedTransformService)
  private val service = new SparkBatchService(classModifiedTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new ClassModifiedTransform(session, service)
    service.run(transformer.transform())
  }

}
