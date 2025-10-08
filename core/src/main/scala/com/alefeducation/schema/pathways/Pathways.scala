package com.alefeducation.schema.pathways

case class PathwayActivityCompletedEvent(learnerId: String,
                                         classId: String,
                                         pathwayId: String,
                                         levelId: String,
                                         activityId: String,
                                         activityType: String,
                                         occurredOn: String,
                                         learnerPathwayClassId: String,
                                         score: Double,
                                         uuid: String,
                                         timeSpent: Int,
                                         learningSessionId: String,
                                         attempt: Int,
                                         academicYear: String)

case class LevelCompletedEvent(learnerId: String,
                               classId: String,
                               pathwayId: String,
                               levelId: String,
                               levelName: String,
                               totalStars: Int,
                               completedOn: String,
                               occurredOn: String,
                               learnerPathwayClassId: String,
                               adtLearningSessionId: String,
                               uuid: String,
                               score: Double,
                               academicYear: String)

case class LevelsRecommendedEvent(learnerId: String,
                                  classId: String,
                                  pathwayId: String,
                                  completedLevelId: String,
                                  recommendedLevels: List[RecommendedLevel],
                                  recommendedOn: String,
                                  recommendationType: String,
                                  occurredOn: String,
                                  learnerPathwayClassId: String,
                                  uuid: String,
                                  academicYear: String)

case class RecommendedLevel(id: String, name: String, status: String)

case class DomainGrade(domainName: String, grade: Int)
case class StudentDomainGradeChangedEvent(
    studentId: String,
    classId: String,
    pathwayId: String,
    currentDomainGrade: Seq[DomainGrade],
    newDomainGrade: Seq[DomainGrade],
    reason: String,
    createdBy: String,
    createdOn: String,
    notifyStudent: Boolean,
    notificationCustomMessage: String,
    occurredOn: String,
    academicYear: String
)

case class StudentManualPlacementEvent(
    studentId: String,
    classId: String,
    pathwayId: String,
    placedBy: String,
    createdOn: String,
    overallGrade: Int,
    occurredOn: String,
    domainGrades: Seq[DomainGrade],
    academicYear: String
)

case class ActivityProgressStatus(
    activityId: String,
    progressStatus: String,
    activityType: String
)

case class PathwayActivitiesAssignedEvent(
    studentId: String,
    classId: String,
    pathwayId: String,
    levelId: String,
    activityIds: Set[String],
    assignedBy: String,
    assignedOn: String,
    occurredOn: String,
    dueDate: DueDate,
    activitiesProgressStatus: List[ActivityProgressStatus]
)

case class DueDate(startDate: String, endDate: String)

case class PathwaysActivityUnAssignedEvent(
    studentId: String,
    pathwayId: String,
    classId: String,
    levelId: String,
    activityId: String,
    unAssignedBy: String,
    unAssignedOn: String,
    occurredOn: String,
)

case class PlacementCompletionEvent(
    learnerId: String,
    pathwayId: String,
    classId: String,
    recommendationType: String,
    gradeLevel: Int,
    domainGrades: List[DomainGrade],
    recommendedLevels: List[RecommendedLevel],
    academicYear: String,
    occurredOn: String,
    placedBy: String = null,
    isInitial: Boolean,
    hasAcceleratedDomains: Boolean
)

case class PlacementTestCompletedEvent(
    attemptNumber: Int,
    learnerPathwayClassId: String,
    learnerId: String,
    classId: String,
    pathwayId: String,
    placementTestId: String,
    startedOn: String,
    completedOn: String,
    score: Double,
    learningSessionId: String,
    schoolId: String,
    academicYear: String,
    occurredOn: String
)

case class AdditionalResourcesAssignedEvent(
    id: String, // mongoDB id of the corresponding document
    learnerId: String,
    courseId: String,
    courseType: String,
    academicYear: String,
    activities: Set[String],
    dueDate: DueDate,
    assignedBy: String,
    pathwayProgressStatus: String, // check enum PathwayProgressStatus for current possible values
    assignedOn: String,
    occurredOn: String,
    uuid: String, // unique message identifier
    resourceInfo: List[ActivityProgressStatus]
)

case class AdditionalResourceUnAssignedEvent(
    id: String,
    learnerId: String,
    courseId: String,
    courseType: String,
    academicYear: String,
    activityId: String,
    unAssignedBy: String,
    unAssignedOn: String,
    occurredOn: String,
    uuid: String // unique message identifier
)

object PathwayProgressStatus extends Enumeration {
  type PathwayProgressStatus = Value

  val PATHWAY_STARTED, DIAGNOSTIC_IN_PROGRESS, DIAGNOSTIC_TEST_LOCKED, DIAGNOSTIC_TEST_UNLOCKED, AWAITING_MANUAL_PLACEMENT,
  AWAITING_DIAGNOSTIC_PLACEMENT = Value
}
