package com.alefeducation.schema.ccl

case class PathwayDraftCreated(
    id: String,
    name: String,
    code: String,
    organisation: String,
    langCode: String,
    occurredOn: Long,
    curriculums: List[Curriculum],
    configuration: Configuration
)

case class PathwayInReviewSchema(
    id: String,
    name: String,
    subjectId: Int,
    subjectIds: List[Int],
    gradeIds: List[Int],
    code: String,
    organisation: String,
    description: String,
    goal: String,
    langCode: String,
    occurredOn: Long,
    curriculums: List[Curriculum],
    configuration: Configuration
)

case class PathwayPublishedSchema(
    id: String,
    organisation: String,
    name: String,
    createdOn: String,
    code: String,
    subOrganisations: List[String],
    subjectId: Int,
    subjectIds: List[Int],
    gradeIds: List[Int],
    description: String,
    goal: String,
    modules: List[PathwayModule],
    langCode: String,
    occurredOn: Long,
    curriculums: List[Curriculum],
    configuration: Configuration
)

case class PathwayModule(id: String, maxAttempts: Int, activityId: ActivityId, settings: PathwaySettings)

case class PathwaySettings(pacing: String, hideWhenPublishing: Boolean, isOptional: Boolean)

case class PathwayDetailsUpdatedSchema(
    id: String,
    status: String,
    `type`: String,
    name: String,
    code: String,
    subjectId: Int,
    subjectIds: List[Int],
    gradeIds: List[Int],
    langCode: String,
    occurredOn: Long,
    courseStatus: String,
    curriculums: List[Curriculum],
    configuration: Configuration
)

case class PathwayDeletedSchema(id: String, status: String, occurredOn: Long, courseStatus: String)

//Level
case class LevelPublishedWithPathwaySchema(
    id: String,
    pathwayId: String,
    index: Int,
    title: String,
    settings: PathwaySettings,
    items: List[LevelItem],
    longName: String,
    description: String,
    metadata: Metadata,
    sequences: List[LevelSequence],
    courseVersion: String,
    occurredOn: Long,
    isAccelerated: Boolean,
)

case class LevelAddedInPathwaySchema(
    id: String,
    pathwayId: String,
    index: Int,
    title: String,
    settings: PathwaySettings,
    longName: String,
    description: String,
    metadata: Metadata,
    sequences: Seq[LevelSequence],
    courseVersion: String,
    courseStatus: String,
    occurredOn: Long,
    isAccelerated: Boolean
)

case class LevelDeletedFromPathwaySchema(
    id: String,
    pathwayId: String,
    index: Int,
    courseVersion: String,
    courseStatus: String,
    occurredOn: Long
)

case class LevelItem(
    id: String,
    settings: PathwaySettings,
    mappedLearningOutcomes: List[LearningOutcome],
    isJointParentActivity: Boolean,
    `type`: String
)

case class LevelSequence(domain: String, sequence: Int)

//ADT
case class ADTPlannedInCourseSchema(
    activityId: ActivityId,
    courseId: String,
    courseType: String,
    maxAttempts: Int,
    settings: PathwaySettings,
    parentItemId: String,
    index: Int,
    courseVersion: String,
    courseStatus: String,
    parentItemType: String,
    occurredOn: Long
)

case class ADTUnPlannedInCourseSchema(
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
