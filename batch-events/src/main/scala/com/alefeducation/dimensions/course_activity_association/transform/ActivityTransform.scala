package com.alefeducation.dimensions.course_activity_association.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.course_activity_association.transform.ActivityAssociationTransformations.createEmptyActivity
import com.alefeducation.dimensions.course_activity_association.transform.CourseActivityAssociationTransform._
import com.alefeducation.dimensions.course_activity_association.transform.{ActivityAssociationTransformations => transformations}
import com.alefeducation.util.Constants.{ActivityPlannedInCourseEvent, ActivityUnPlannedInCourseEvent, ActivityUpdatedInCourseEvent}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ActivityTransform(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._

  def transform(sourceNames: List[String]): Option[DataFrame] = {
    val activitySources: List[DataFrame] = sourceNames.flatMap(service.readOptional(_, session))
    val transformedActivities: List[DataFrame] = activitySources.map(_.transform(applyActivityAssociationTransformations("activityId.uuid")))

    val unitedActivity: Option[DataFrame] = uniteActivities(transformedActivities)

    unitedActivity.map(
      _.transformForIWH2(
        columnMapping = ActivityColumns,
        entity = CourseActivityAssociationEntity,
        associationType = ActivityType,
        attachedEvents = List(ActivityPlannedInCourseEvent, ActivityUpdatedInCourseEvent),
        detachedEvents = List(ActivityUnPlannedInCourseEvent),
        groupKey = List(s"${CourseActivityAssociationEntity}_activity_id", "courseId"),
        associationAttachedStatusVal = ActivityPlannedStatus,
        associationDetachedStatusVal = ActivityUnplannedStatus,
        inactiveStatus = CourseActivityAssociationInactiveStatus
      )
    )
  }

  private def uniteActivities(transformedActivities: List[DataFrame]): Option[DataFrame] = {
    val emptyActivityAccumulator: DataFrame = createEmptyActivity(session).transform(applyActivityAssociationTransformations("activityId.uuid"))
    transformedActivities
      .foldLeft(emptyActivityAccumulator) {
        case (accumulator, nextActivity) => accumulator.unionByName(nextActivity)
      }
      .checkEmptyDf
  }

  private def applyActivityAssociationTransformations(activityField: String): DataFrame => DataFrame =
    transformations.addActivityId(CourseActivityAssociationEntity, activityField) _ andThen
      transformations.dropActivityDuplicates(CourseActivityAssociationEntity) andThen
      transformations.addActivityType(ActivityType) andThen
      transformations.addSettingsOrDefaultValue andThen
      transformations.addIsParentDeleted andThen
      transformations.addIsJointParentActivity andThen
      transformations.addGrade(s"${CourseActivityAssociationEntity}_grade", session) andThen
      transformations.selectRequiredColumns(ActivitySourceColumns)

  private val ActivitySourceColumns: List[String] = List(
    "parentItemId",
    "courseId",
    s"${CourseActivityAssociationEntity}_activity_id",
    s"${CourseActivityAssociationEntity}_activity_type",
    s"${CourseActivityAssociationEntity}_activity_pacing",
    s"${CourseActivityAssociationEntity}_activity_is_optional",
    "index",
    s"${CourseActivityAssociationEntity}_is_parent_deleted",
    "courseVersion",
    s"${CourseActivityAssociationEntity}_is_joint_parent_activity",
    "metadata",
    s"${CourseActivityAssociationEntity}_grade",
    "occurredOn",
    "eventType"
  )

  private val ActivityColumns: Map[String, String] = Map(
    s"${CourseActivityAssociationEntity}_status" -> s"${CourseActivityAssociationEntity}_status",
    s"${CourseActivityAssociationEntity}_attach_status" -> s"${CourseActivityAssociationEntity}_attach_status",
    "parentItemId" -> s"${CourseActivityAssociationEntity}_container_id",
    "courseId" -> s"${CourseActivityAssociationEntity}_course_id",
    s"${CourseActivityAssociationEntity}_activity_id" -> s"${CourseActivityAssociationEntity}_activity_id",
    s"${CourseActivityAssociationEntity}_activity_type" -> s"${CourseActivityAssociationEntity}_activity_type",
    s"${CourseActivityAssociationEntity}_activity_pacing" -> s"${CourseActivityAssociationEntity}_activity_pacing",
    s"${CourseActivityAssociationEntity}_activity_is_optional" -> s"${CourseActivityAssociationEntity}_activity_is_optional",
    "index" -> s"${CourseActivityAssociationEntity}_activity_index",
    s"${CourseActivityAssociationEntity}_is_parent_deleted" -> s"${CourseActivityAssociationEntity}_is_parent_deleted",
    "courseVersion" -> s"${CourseActivityAssociationEntity}_course_version",
    s"${CourseActivityAssociationEntity}_is_joint_parent_activity" -> s"${CourseActivityAssociationEntity}_is_joint_parent_activity",
    "metadata" -> s"${CourseActivityAssociationEntity}_metadata",
    s"${CourseActivityAssociationEntity}_grade" -> s"${CourseActivityAssociationEntity}_grade",
    "occurredOn" -> "occurredOn",
  )
}
