package com.alefeducation.dimensions.course_activity_association.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.course_activity_association.transform.CourseActivityAssociationTransform.ActivityType
import com.alefeducation.dimensions.course_activity_association.transform.CourseActivityGradeAssociationTransform._
import com.alefeducation.dimensions.course_activity_association.transform.{ActivityAssociationTransformations => transformations}
import com.alefeducation.schema.ccl.Tag
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants._
import com.alefeducation.util.Resources.{getList, getNestedMap, getNestedString, getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class CourseActivityGradeAssociationTransform(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._

  def transform(serviceName: String): Option[Sink] = {
    val plannedSource = getNestedString(serviceName, "planned-source")
    val updatedSource = getNestedString(serviceName, "updated-source")
    val activityField = getNestedString(serviceName, "activity-field")
    val activityIdField = getNestedString(serviceName, "activity-id-field")
    val attachEvents = getList(serviceName, "attach-events")
    val associationCols = getNestedMap(serviceName, "associationCols")
    val entity = getNestedString(serviceName, "entity")
    val key = getNestedString(serviceName, "key")
    val selectedColList = getList(serviceName, "selectedColList")

    val sinkName = getSink(serviceName).head

    val plannedEvents = service.readOptional(plannedSource, session).map(_.select(activityField, "eventType", "occurredOn", "metadata", "courseId", "courseVersion"))
    val updatedEvents = service.readOptional(updatedSource, session).map(_.select(activityField, "eventType", "occurredOn", "metadata", "courseId", "courseVersion"))

    val transformedActivityOutcomes = plannedEvents.unionOptionalByName(updatedEvents)
      .map(_.transform(transformGrades(activityIdField, entity, selectedColList)))

    val startId = service.getStartIdUpdateStatus(key)

    val activityGrade = transformedActivityOutcomes.map(
      _.transformForIWH2(
        associationCols,
        entity,
        associationType = ActivityType,
        attachedEvents = attachEvents,
        detachedEvents = Nil,
        groupKey = List(s"${entity}_activity_id", "courseId"),
        associationAttachedStatusVal = PathwayLevelActivityPlannedStatus,
        associationDetachedStatusVal = PathwayLevelActivityUnPlannedStatus,
        inactiveStatus = PathwayLevelActivityAssociationInactiveStatus
      ).genDwId(
        s"${entity}_dw_id",
        maxId = startId,
        orderByField = s"${entity}_created_time"
      )
    )

    activityGrade.map(
      DataSink(sinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> key))
    )
  }

  private def transformGrades(activityFieldName: String, entity: String, selectedColList: List[String]): DataFrame => DataFrame =
    transformations.addActivityId(entity, activityFieldName) _ andThen
      transformations.dropActivityDuplicates(entity) andThen
      explodeGrade andThen
      transformations.selectRequiredColumns(selectedColList)

  private def explodeGrade(df: DataFrame): DataFrame = {
    val nullTagsCol = lit(typedLit(Seq.empty[Tag]))

    val activityWithTags = if (!df.columns.contains(TagsColumnName)) {
      df.withColumn(TagsColumnName, nullTagsCol)
    } else {
      df.withColumn(
        TagsColumnName,
        when(col(TagsColumnName).isNull, nullTagsCol).otherwise(col(TagsColumnName))
      )
    }

    activityWithTags
      .select(col("*"), explode_outer(col(TagsColumnName)).as("grades"))
      .select(col("*"), col("grades.*"))
      .where(col("key") === lit("Grade"))
      .select(col("*"), explode_outer(col("values")).as("grade"))
  }

}

object CourseActivityGradeAssociationTransform {

  val CourseActivityGradeAssociationService = "transform-course-activity-grade-association"

  val TagsColumnName = "metadata.tags"


  def main(args: Array[String]): Unit = {
    val serviceName = args(0)
    val session: SparkSession = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)

    val transform = new CourseActivityGradeAssociationTransform(session, service)
    service.run(transform.transform(serviceName))
  }
}
