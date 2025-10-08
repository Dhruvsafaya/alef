package com.alefeducation.dimensions.course_activity_association.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.course_activity_association.transform.CourseActivityAssociationTransform.ActivityType
import com.alefeducation.dimensions.course_activity_association.transform.CourseActivityOutcomeAssociationTransform._
import com.alefeducation.dimensions.course_activity_association.transform.{ActivityAssociationTransformations => transformations}
import com.alefeducation.schema.ccl.LearningOutcome
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.Constants._
import com.alefeducation.util.Resources.{getList, getNestedMap, getNestedString, getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class CourseActivityOutcomeAssociationTransform(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._

  def transform(serviceName: String): Option[Sink] = {

    val plannedSource = getNestedString(serviceName, "planned-source")
    val updatedSource = getNestedString(serviceName, "updated-source")
    val sinkName = getSink(serviceName).head
    val activityField = getNestedString(serviceName, "activity-field")
    val activityIdField = getNestedString(serviceName, "activity-id-field")
    val attachEvents = getList(serviceName, "attach-events")
    val associationCols = getNestedMap(serviceName, "associationCols")
    val entity = getNestedString(serviceName, "entity")
    val key = getNestedString(serviceName, "key")
    val selectedColList = getList(serviceName, "selectedColList")

    val plannedEvents = service.readOptional(plannedSource, session).map(_.select(activityField, "eventType", "occurredOn", "mappedLearningOutcomes", "courseId", "courseVersion"))
    val updatedEvents = service.readOptional(updatedSource, session).map(_.select(activityField, "eventType", "occurredOn", "mappedLearningOutcomes", "courseId", "courseVersion"))

    val transformedActivityOutcomes = plannedEvents.unionOptionalByName(updatedEvents)
    .map(_.transform(transformOutcomes(activityIdField, entity, selectedColList)))

    val startId = service.getStartIdUpdateStatus(key)

    val activityOutcome = transformedActivityOutcomes.map(
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

    activityOutcome.map(
      DataSink(sinkName, _, controlTableUpdateOptions = Map(ProductMaxIdType -> key))
    )
  }

  private def transformOutcomes(activityField: String, entity: String, selectedColList: List[String]): DataFrame => DataFrame =
    transformations.addActivityId(entity, activityField) _ andThen
      transformations.dropActivityDuplicates(entity) andThen
      explodeLearningOutcome andThen
      transformations.selectRequiredColumns(selectedColList) andThen
        transformations.castIdToStr

  private def explodeLearningOutcome(df: DataFrame): DataFrame = {
    val nullLearningOutcomeCol = lit(typedLit(Seq.empty[LearningOutcome]))

    val activityWithMappedOutcomes = if (!df.columns.contains(MappedOutcomesColumnName)) {
      df.withColumn(MappedOutcomesColumnName, nullLearningOutcomeCol)
    } else {
      df.withColumn(
        MappedOutcomesColumnName,
        when(col(MappedOutcomesColumnName).isNull, nullLearningOutcomeCol).otherwise(col(MappedOutcomesColumnName))
      )
    }

    activityWithMappedOutcomes
      .select(col("*"), explode_outer(col(MappedOutcomesColumnName)).as("outcome"))
      .select(col("*"), col("outcome.*"))
  }

}

object CourseActivityOutcomeAssociationTransform {

  val CourseActivityOutcomeAssociationService = "transform-course-activity-outcome-association"

  val MappedOutcomesColumnName = "mappedLearningOutcomes"

  def main(args: Array[String]): Unit = {
    val serviceName = args(0)
    val session: SparkSession = SparkSessionUtils.getSession(serviceName)
    val service = new SparkBatchService(serviceName, session)
    val transform = new CourseActivityOutcomeAssociationTransform(session, service)
    service.run(transform.transform(serviceName))
  }
}
