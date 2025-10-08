package com.alefeducation.dimensions.course_activity_association.transform

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.course_activity_association.transform.CourseActivityAssociationTransform._
import com.alefeducation.dimensions.course_activity_association.transform.{ActivityAssociationTransformations => transformations}
import com.alefeducation.schema.ccl.InterimCheckpointPublishedSchema
import com.alefeducation.util.Constants.{InterimCheckpointDeletedEvent, InterimCheckpointPublishedEvent, InterimCheckpointUpdatedEvent}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}

class InterimCheckpointTransform(val session: SparkSession, val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._
  import session.implicits._

  def transform(sourceNames: List[String]): Option[DataFrame] = {
    val interimCheckpointSources: List[DataFrame] = sourceNames.flatMap(
      service.readUniqueOptional(_, session, uniqueColNames = List("courseId", "id", "courseVersion"))
    )
    val transformedInterimCheckpoints: List[DataFrame] = interimCheckpointSources.map(_.transform(applyInterimCheckpointTransformations))

    // Create an empty df to use as initial accumulator in foldLeft.
    // Apply the transformations on empty dataframe to add required fields for union
    val emptyInterimCheckpoint: DataFrame = createEmptyInterimCheckpoint.transform(applyInterimCheckpointTransformations)
    val unitedInterimCheckpoint: Option[DataFrame] = transformedInterimCheckpoints
      .foldLeft(emptyInterimCheckpoint) {
        case (accumulator, nextInterimCheckpoint) => accumulator.unionByName(nextInterimCheckpoint)
      }
      .checkEmptyDf

    unitedInterimCheckpoint.map(
      _.transformForIWH2(
        InterimCheckpointColumns,
        CourseActivityAssociationEntity,
        InterimCheckpointType,
        List(InterimCheckpointPublishedEvent, InterimCheckpointUpdatedEvent),
        List(InterimCheckpointDeletedEvent),
        List("id", "courseId"),
        associationAttachedStatusVal = ActivityPlannedStatus,
        associationDetachedStatusVal = ActivityUnplannedStatus,
        inactiveStatus = CourseActivityAssociationInactiveStatus
      )
    )
  }

  private def applyInterimCheckpointTransformations: DataFrame => DataFrame =
    transformations.addActivityType(InterimCheckpointType) _ andThen
      transformations.addSettingsOrDefaultValue andThen
      transformations.addIsParentDeleted andThen
      transformations.addIsJointParentActivity andThen
      transformations.addEmptyMetadata andThen
      transformations.addEmptyGrade andThen
      transformations.selectRequiredColumns(InterimCheckpointSourceColumns)


  private def createEmptyInterimCheckpoint: DataFrame = {
    val nullStr = lit(null).cast(StringType)
    Seq
      .empty[InterimCheckpointPublishedSchema]
      .toDF()
      .withColumn("eventType", nullStr)
  }

  private val InterimCheckpointSourceColumns: List[String] = List(
    "parentItemId",
    "courseId",
    "id",
    s"${CourseActivityAssociationEntity}_activity_type",
    s"${CourseActivityAssociationEntity}_activity_pacing",
    s"${CourseActivityAssociationEntity}_activity_is_optional",
    "index",
    s"${CourseActivityAssociationEntity}_is_parent_deleted",
    "courseVersion",
    s"${CourseActivityAssociationEntity}_is_joint_parent_activity",
    s"${CourseActivityAssociationEntity}_metadata",
    s"${CourseActivityAssociationEntity}_grade",
    "occurredOn",
    "eventType"
  )

  private val InterimCheckpointColumns: Map[String, String] = Map(
    s"${CourseActivityAssociationEntity}_status" -> s"${CourseActivityAssociationEntity}_status",
    s"${CourseActivityAssociationEntity}_attach_status" -> s"${CourseActivityAssociationEntity}_attach_status",
    "parentItemId" -> s"${CourseActivityAssociationEntity}_container_id",
    "courseId" -> s"${CourseActivityAssociationEntity}_course_id",
    "id" -> s"${CourseActivityAssociationEntity}_activity_id",
    s"${CourseActivityAssociationEntity}_activity_type" -> s"${CourseActivityAssociationEntity}_activity_type",
    s"${CourseActivityAssociationEntity}_activity_pacing" -> s"${CourseActivityAssociationEntity}_activity_pacing",
    s"${CourseActivityAssociationEntity}_activity_is_optional" -> s"${CourseActivityAssociationEntity}_activity_is_optional",
    "index" -> s"${CourseActivityAssociationEntity}_activity_index",
    s"${CourseActivityAssociationEntity}_is_parent_deleted" -> s"${CourseActivityAssociationEntity}_is_parent_deleted",
    "courseVersion" -> s"${CourseActivityAssociationEntity}_course_version",
    s"${CourseActivityAssociationEntity}_is_joint_parent_activity" -> s"${CourseActivityAssociationEntity}_is_joint_parent_activity",
    s"${CourseActivityAssociationEntity}_metadata" -> s"${CourseActivityAssociationEntity}_metadata",
    s"${CourseActivityAssociationEntity}_grade" -> s"${CourseActivityAssociationEntity}_grade",
    "occurredOn" -> "occurredOn",
  )

}
