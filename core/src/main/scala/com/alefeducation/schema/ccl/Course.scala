package com.alefeducation.schema.ccl

import org.apache.spark.sql.types._

case class Curriculum(curriculumId: Long, gradeId: Long, subjectId: Long)

case class ActivityId(uuid: String, id: Int)

case class Metadata(tags: List[Tag])
case class Tag(key: String, values: List[String], attributes: List[ListTagAttributes], `type`: String)
case class ListTagAttributes(value: String, color: String, translation: String)

case class CourseSettings(pacing: String, hideWhenPublishing: Boolean, isOptional: Boolean)

case class AbilityTestModule(id: String, maxAttempts: Int, activityId: ActivityId, settings: CourseSettings)

case class LearningOutcome(
                            id: Long,
                            `type`: String,
                            curriculumId: Long,
                            gradeId: Long,
                            subjectId: Long
                          )

case class SequenceProjection(domain: String, sequence: Int)

case class CourseItemEventProjection(
                      id: String,
                      settings: PathwaySettings,
                      mappedLearningOutcomes: List[LearningOutcome],
                      isJointParentActivity: Boolean,
                      `type`: String
                    )

case class LessonsWithPools(
                             id: String,
                             poolIds: Seq[String]
                           )
case class Configuration(
                          programEnabled: Boolean,
                          resourcesEnabled: Boolean,
                          placementType: String
                        )
case class FileInfo(
  fileName: String,
  fileId: String,
  path: String
)

case class DraftCourseCreated(
    id: String,
    name: String,
    code: String,
    organisation: String,
    langCode: String,
    occurredOn: Long,
    curriculums: List[Curriculum],
    courseVersion: String,
    courseType: String,
    configuration: Configuration
)

case class CourseInReviewSchema(
    id: String,
    name: String,
    subjectIds: List[Int],
    gradeIds: List[Int],
    code: String,
    organisation: String,
    description: String,
    goal: String,
    langCode: String,
    occurredOn: Long,
    curriculums: List[Curriculum],
    courseVersion: String,
    courseType: String,
    configuration: Configuration
)

case class CoursePublishedSchema(
    id: String,
    courseType: String,
    organisation: String,
    name: String,
    createdOn: String,
    code: String,
    subOrganisations: List[String],
    subjectIds: List[Int],
    gradeIds: List[Int],
    description: String,
    goal: String,
    modules: List[AbilityTestModule],
    langCode: String,
    occurredOn: Long,
    curriculums: List[Curriculum],
    instructionalPlanIds: List[String],
    configuration: Configuration,
    courseVersion: String,
)

case class CourseSettingsUpdatedSchema(
      id: String,
      courseType: String,
      status: String,
      `type`: String,
      name: String,
      code: String,
      subjectIds: List[Int],
      gradeIds: List[Int],
      langCode: String,
      occurredOn: Long,
      courseStatus: String,
      curriculums: List[Curriculum],
      configuration: Configuration,
      courseVersion: String,
)

case class CourseDeletedSchema(id: String, courseType: String, status: String, occurredOn: Long, courseStatus: String)

//Activity
case class ActivityPlannedInCourseSchema(
    activityId: ActivityId,
    courseId: String,
    courseType: String,
    parentItemId: String,
    index: Int,
    courseVersion: String,
    settings: CourseSettings,
    courseStatus: String,
    parentItemType: String,
    occurredOn: Long,
    isJointParentActivity: Boolean,
    mappedLearningOutcomes: List[LearningOutcome],
    metadata: Metadata
)

case class ActivityUpdatedInCourseSchema(
    activityId: ActivityId,
    courseId: String,
    courseType: String,
    parentItemId: String,
    index: Int,
    courseVersion: String,
    settings: CourseSettings,
    courseStatus: String,
    parentItemType: String,
    occurredOn: Long,
    isParentDeleted: Boolean,
    isJointParentActivity: Boolean,
    mappedLearningOutcomes: List[LearningOutcome],
    metadata: Metadata
)

case class ActivityUnPlannedInCourseSchema(
    activityId: ActivityId,
    courseId: String,
    courseType: String,
    parentItemId: String,
    index: Int,
    courseVersion: String,
    courseStatus: String,
    parentItemType: String,
    occurredOn: Long,
    isParentDeleted: Boolean,
    isJointParentActivity: Boolean,
    mappedLearningOutcomes: List[LearningOutcome],
    metadata: Metadata
)

// container
case class ContainerPublishedWithCourseSchema(
    id: String,
    `type`: String,
    courseId: String,
    courseType: String,
    index: Int,
    title: String,
    settings: CourseSettings,
    items: List[CourseItemEventProjection],
    longName: String,
    description: String,
    metadata: Metadata,
    sequences: List[SequenceProjection],
    courseVersion: String,
    occurredOn: Long,
    isAccelerated: Boolean,
)

case class ContainerAddedInCourseSchema(
    id: String,
    `type`: String,
    courseId: String,
    courseType: String,
    index: Int,
    title: String,
    settings: CourseSettings,
    longName: String,
    description: String,
    metadata: Metadata,
    sequences: List[SequenceProjection],
    courseVersion: String,
    courseStatus: String,
    occurredOn: Long,
    isAccelerated: Boolean
)

case class ContainerDeletedFromCourseSchema(
    id: String,
    `type`: String,
    courseId: String,
    courseType: String,
    index: Int,
    courseVersion: String,
    courseStatus: String,
    occurredOn: Long
)

//ADT

case class AbilityTestComponentEnabledSchema(
    id: String,
    `type`: String,
    courseId: String,
    courseType: String,
    courseVersion: String,
    courseStatus: String,
    occurredOn: Long
)

case class AbilityTestComponentDisabledSchema(
    id: String,
    `type`: String,
    courseId: String,
    courseType: String,
    courseVersion: String,
    courseStatus: String,
    occurredOn: Long
)

case class ActivityPlannedInAbilityTestComponentSchema(
   activityId: ActivityId,
   courseId: String,
   courseType: String,
   maxAttempts: Int,
   settings: CourseSettings,
   parentItemId: String,
   index: Int,
   courseVersion: String,
   courseStatus: String,
   parentItemType: String,
   occurredOn: Long
)

case class ActivityUnPlannedInAbilityTestComponentSchema(
   activityId: ActivityId,
   courseId: String,
   courseType: String,
   parentItemId: String,
   index: Int,
   courseVersion: String,
   courseStatus: String,
   parentItemType: String,
   occurredOn: Long
)

case class AbilityTestComponentUpdatedSchema(
    activityId: ActivityId,
    courseId: String,
    courseType: String,
    parentItemId: String,
    maxAttempts: Int,
    settings: CourseSettings,
    index: Int,
    courseVersion: String,
    courseStatus: String,
    parentItemType: String,
    occurredOn: Long
)

object CourseBaseSchemas {
  val courseSettingsSchema: StructType = StructType(
    Seq(
      StructField("pacing", StringType),
      StructField("hideWhenPublishing", BooleanType),
      StructField("isOptional", BooleanType),
    )
  )

  val metadataSchema: StructType = StructType(
    Seq(
      StructField("tags",
        ArrayType(
          StructType(
            Seq(
              StructField("key", StringType),
              StructField("values", ArrayType(StringType)),
              StructField("attributes", ArrayType(
                StructType(
                  Seq(
                    StructField("value", StringType),
                    StructField("color", StringType),
                    StructField("translation", StringType)
                  )
                )
              )),
              StructField("type", StringType)
            )
          )
        )
      ),
      StructField("version", StringType),
    )
  )
}

object TestPlannedInAbilityTestComponentSchema {
  val testPlannedInAbilityTestComponentSchema: StructType = StructType(
    Seq(
      StructField("id", StringType),
      StructField("type", StringType),
      StructField("maxAttempts", IntegerType),
      StructField("settings", CourseBaseSchemas.courseSettingsSchema),
      StructField("metadata", CourseBaseSchemas.metadataSchema),
      StructField("legacyId", IntegerType),
      StructField("parentComponentId", StringType),
      StructField("index", IntegerType),
      StructField("courseId", StringType),
      StructField("courseType", StringType),
      StructField("courseVersion", StringType),
      StructField("courseStatus", StringType),
      StructField("parentComponentType", StringType),
      StructField("isPlacementTest", BooleanType),
      StructField("occurredOn", LongType),
    )
  )
}

//Interim Checkpoint

case class InterimCheckpointPublishedSchema(
   id: String,
   courseId: String,
   title: String,
   language: String,
   userTenant: String,
   lessonsWithPools: Seq[LessonsWithPools],
   createdOn: String,
   updatedOn: String,
   settings: CourseSettings,
   parentItemId: String,
   index: Int,
   courseVersion: String,
   courseType: String,
   parentItemType: String,
   courseStatus: String,
   occurredOn: Long
)

case class InterimCheckpointUpdatedSchema(
   id: String,
   courseId: String,
   title: String,
   language: String,
   userTenant: String,
   lessonsWithPools: Seq[LessonsWithPools],
   createdOn: String,
   updatedOn: String,
   settings: CourseSettings,
   parentItemId: String,
   index: Int,
   courseVersion: String,
   courseType: String,
   parentItemType: String,
   courseStatus: String,
   occurredOn: Long,
   status: String,
   isParentDeleted: Boolean
)

case class InterimCheckpointDeletedEvent(
    id: String,
    userTenant: String,
    parentItemId: String,
    parentItemType: String,
    index: Int,
    courseId: String,
    courseVersion: String,
    courseStatus: String,
    occurredOn: Long,
    isParentDeleted: Boolean
)

// Pacing-Guide
case class PacingGuideCreatedEventSchema(
    id: String,
    classId: String,
    courseId: String,
    instructionalPlanId: Option[String],
    academicYearId: Option[String],
    academicCalendarId: Option[String],
    activities: List[CourseContentActivity],
    occurredOn: Long,
)

case class PacingGuideUpdatedEventSchema(
    id: String,
    classId: String,
    courseId: String,
    instructionalPlanId: Option[String],
    academicYearId: Option[String],
    academicCalendarId: Option[String],
    activities: List[CourseContentActivity],
    occurredOn: Long,
)

case class PacingGuideDeletedEventSchema(
    id: String,
    classId: String,
    courseId: String,
    instructionalPlanId: Option[String],
    activities: List[CourseContentActivity],
    occurredOn: Long,
)

case class CourseContentActivity(
  activity: CourseActivity,
  associations: List[Association],
  activityState: ActivityState,
)

case class CourseActivity(
  id: String,
  `type`: String,
  order: Option[Int],
  metadata: Option[Metadata],
  mappedLearningOutcomes: List[LearningOutcome],
  isJointParentActivity: Boolean = false,
  settings: CourseSettings,
  legacyId: Long = 0,
)

case class Association(
  id: String,
  `type`: String,
  label: String,
  startDate: Option[String],
  endDate: Option[String],
)

case class ActivityState(
  lockedStatus: String,
  highlighted: Boolean,
  modifiedOn: String,
  modifiedBy: String
)

// Course Instructional Plan
case class InstructionalPlanPublishedEvent(
  id: String,
  courseId: String,
  courseType: String,
  courseVersion: String,
  courseStatus: String,
  timeFrames: List[TimeFrameEventProjection],
  occurredOn: Long
)

case class InstructionalPlanInReviewEvent(
  id: String,
  courseId: String,
  courseType: String,
  courseVersion: String,
  courseStatus: String,
  timeFrames: List[TimeFrameEventProjection],
  occurredOn: Long
)

case class InstructionalPlanDeletedEvent(
  id: String,
  courseId: String,
  courseType: String,
  courseVersion: String,
  courseStatus: String,
  occurredOn: Long
)

case class AdditionalResourceActivityPlannedSchema(
  resourceId: String,
  courseId: String,
  courseType: String,
  courseVersion: String,
  courseStatus: String,
  occurredOn: Long,
  mappedLearningOutcomes: List[LearningOutcome],
  metadata: Metadata,
  legacyId: Long,
  `type`: String
)

case class AdditionalResourceActivityUpdatedSchema(
  resourceId: String,
  courseId: String,
  courseType: String,
  courseVersion: String,
  courseStatus: String,
  occurredOn: Long,
  mappedLearningOutcomes: List[LearningOutcome],
  metadata: Metadata,
  legacyId: Long,
  `type`: String
)

case class AdditionalResourceActivityUnplannedSchema(
  resourceId: String,
  courseId: String,
  courseType: String,
  courseVersion: String,
  courseStatus: String,
  occurredOn: Long,
  mappedLearningOutcomes: List[LearningOutcome],
  metadata: Metadata,
  legacyId: Long,
  `type`: String
)

case class DownloadableResourcePlannedSchema(
  resourceId: String,
  courseId: String,
  courseType: String,
  courseVersion: String,
  courseStatus: String,
  occurredOn: Long,
  metadata: Metadata,
  `type`: String,
  description: String,
  title: String,
  fileInfo: FileInfo
)

case class DownloadableResourceUpdatedSchema(
  resourceId: String,
  courseId: String,
  courseType: String,
  courseVersion: String,
  courseStatus: String,
  occurredOn: Long,
  metadata: Metadata,
  `type`: String,
  description: String,
  title: String,
  fileInfo: FileInfo
)

case class DownloadableResourceUnplannedSchema(
  resourceId: String,
  courseId: String,
  courseType: String,
  courseVersion: String,
  courseStatus: String,
  occurredOn: Long,
  metadata: Metadata,
  `type`: String,
  description: String,
  title: String,
  fileInfo: FileInfo
)


case class TimeFrameEventProjection(
  id: String,
  items: List[InstructionalPlanActivityEventProjection],
)

case class InstructionalPlanActivityEventProjection(
  parentContainer: Option[ParentContainerInfoEventProjection],
  activity: CourseItemEventProjection,
)

case class ParentContainerInfoEventProjection(
  id: String,
  title: String,
)