package com.alefeducation.dimensions.academic_calendar_teaching_period

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.{getList, getNestedMap, getNestedString, getSink}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{asc, col, desc, explode, row_number}

class AcademicCalendarTeachingPeriodTransform(val session: SparkSession, val service: SparkBatchService, val serviceName: String) {
  import com.alefeducation.util.BatchTransformerUtility._
  import com.alefeducation.bigdata.batch.BatchUtils.DataFrameUtils
  import session.implicits._

  def transform(): Option[Sink] = {
    val sourceName = getList(serviceName, "source").head
    val sinkName = getList(serviceName, "sink").head
    val key = getNestedString(serviceName, "key")
    val eventsName = getNestedString(serviceName, "event-name")
    val entityPrefix = getNestedString(serviceName, "entity-prefix")
    val cols = getNestedMap(serviceName, "column-mapping")

    val sourcePublished: Option[DataFrame] =
      service.readOptional(sourceName, session, extraProps = List(("mergeSchema", "true"))).map(_.withColumnRenamed("uuid", "id"))
    val startId = service.getStartIdUpdateStatus(key)
    val partition = Window.partitionBy($"id").orderBy(asc("startDate"))

    val sourceFields: Option[DataFrame] =
      sourcePublished.map(_.withColumn("teachingPeriod", explode(col("teachingPeriods")))
        .select("eventType", "id", "occurredOn", "teachingPeriod.*")
        .epochToDate("startDate")
        .epochToDate("endDate")
        .withColumn("teachingPeriodOrder", row_number().over(partition))
      )
    val sourceCreated = sourceFields.flatMap(
      _.transformForIWH2(
        cols,
        entityPrefix,
        0,
        List(eventsName),
        Nil,
        getList(serviceName, "unique-ids"),
        inactiveStatus = 2
      ).genDwId(s"${entityPrefix}_dw_id", startId)
        .checkEmptyDf
    )

    sourceCreated.map(DataSink(sinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> key)))
  }
}


object AcademicCalendarTeachingPeriodTransform {
  def main(args: Array[String]): Unit = {
    val serviceName = args(0)
    val session: SparkSession = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)
    val transformer = new AcademicCalendarTeachingPeriodTransform(session, service, serviceName)
    service.run(transformer.transform())
  }
}