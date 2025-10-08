package com.alefeducation.dimensions.course.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Constants.CourseInactiveStatusVal
import com.alefeducation.util.Resources._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

class CourseFieldMutatedTransform(val session: SparkSession, val service: SparkBatchService, val serviceName: String) {

  import session.implicits._
  def transform(): List[Sink] = {
    val sources = getSource(serviceName)
    val sinkName = getSink(serviceName).head
    val key = getNestedString(serviceName, "key")
    val eventsName = getList(serviceName, "event-name")
    val entityPrefix = getNestedString(serviceName, "entity-prefix")
    val cols = getNestedMap(serviceName, "colsMap")
    val colToExplode = getNestedString(serviceName, "colToExplode")

    val coursePublished: Option[DataFrame] =
      service.readOptional(sources.head, session, extraProps = List(("mergeSchema", "true")))

    val startId = service.getStartIdUpdateStatus(key)

    val courseDetailsUpdated: Option[DataFrame] =
      service.readOptional(sources.last, session, extraProps = List(("mergeSchema", "true")))

    val courseFieldsPublished: Option[DataFrame] = if (coursePublished.flatMap(_.checkEmptyDf).isEmpty) {
      coursePublished
    } else {
      coursePublished.map(
        _.withColumn(colToExplode, explode(col(colToExplode)))
          .select("eventType", "id", "occurredOn", colToExplode))
    }

    val courseFieldsMutated: Option[DataFrame] = getCourseFieldUpdatedDF(session, colToExplode, courseDetailsUpdated)

    val finalDF = courseFieldsPublished.unionOptionalByNameWithEmptyCheck(courseFieldsMutated, allowMissingColumns = true)

    val fieldsMutated = finalDF.flatMap(
      _.transformForIWH2(
        cols,
        entityPrefix,
        0,
        attachedEvents = eventsName,
        Nil,
        List("id"),
        inactiveStatus = CourseInactiveStatusVal
      ).genDwId(s"${entityPrefix}_dw_id", startId)
        .withColumn(s"${entityPrefix}_dw_updated_time",
                    when(col(s"${entityPrefix}_status") === lit(CourseInactiveStatusVal), current_timestamp()))
        .checkEmptyDf
    )

    fieldsMutated.map(DataSink(sinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> key))).toList
  }

  private def getCourseFieldUpdatedDF(sparkSession: SparkSession, colToExplode: String, courseDetailsUpdated: Option[DataFrame]) = {
    if (courseDetailsUpdated.flatMap(_.checkEmptyDf).isEmpty) {
      courseDetailsUpdated
    } else {
      val courseFieldsUpdated = courseDetailsUpdated
        .flatMap(_.filter(size($"$colToExplode") > 0 and $"courseStatus" === "PUBLISHED").checkEmptyDf)
        .map(
          _.withColumn(colToExplode, explode(col(colToExplode)))
            .select("eventType", "id", "occurredOn", colToExplode)
        )

      val courseWithEmptyFields = courseDetailsUpdated.map(
        _.filter(size($"$colToExplode") === 0 and $"courseStatus" === "PUBLISHED")
          .select("eventType", "id", "occurredOn")
          .withColumn(colToExplode, lit(null).cast(IntegerType)))
      courseFieldsUpdated.unionOptionalByNameWithEmptyCheck(courseWithEmptyFields)
    }
  }
}

object CourseFieldMutatedTransform {

  def main(args: Array[String]): Unit = {
    val serviceName = args(0)

    val session: SparkSession = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)

    val transformer = new CourseFieldMutatedTransform(session, service, serviceName)
    service.runAll(transformer.transform())
  }
}
