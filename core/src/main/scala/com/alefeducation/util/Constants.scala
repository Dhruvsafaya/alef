package com.alefeducation.util

object Constants {

  val DefaultEpochTime = 0
  val MaxPartitions: Int = 2 * 2 //(two node * two core)
  val MinPartitions = 1
  val Format = "format"
  val DbTable = "dbtable"
  val Mode = "mode"
  val Path = "path"
  val OutputMode = "outputMode"
  val PartitionBy = "partitionBy"
  val Trigger = "trigger"
  val Checkpoint = "checkpointLocation"

  val UTCTimeZone = "UTC"

  val HistoryState = 2
  val ActiveState = 1
  val DisableState = 3
  val DeletedState = 4

  // Constants for event type filter
  final val AdminSchoolCreated = "SchoolCreatedEvent"
  final val AdminSchoolUpdated = "SchoolUpdatedEvent"
  final val AdminSchoolDeleted = "SchoolDeletedEvent"
  final val SchoolDeactivated = "SchoolDeactivatedEvent"
  final val SchoolActivated = "SchoolActivatedEvent"
  final val SchoolStatusToggle = "SchoolActivatedDeactivatedEvent"
  final val AdminGradeCreated = "GradeCreatedEvent"
  final val AdminGradeUpdated = "GradeUpdatedEvent"
  final val AdminGradeDeleted = "GradeDeletedEvent"

  final val AdminSectionCreated = "SectionCreatedEvent"
  final val AdminSectionUpdated = "SectionUpdatedEvent"
  final val AdminSectionDeleted = "SectionDeletedEvent"
  final val AdminSectionEnabled = "SectionEnabledEvent"
  final val AdminSectionDisabled = "SectionDisabledEvent"

  final val AdminSubjectCreated = "SubjectCreatedEvent"
  final val AdminSubjectUpdated = "SubjectUpdatedEvent"
  final val AdminSubjectDeleted = "SubjectDeletedEvent"
  final val AdminStudent = "StudentCreatedEvent"
  final val AdminStudentUpdated = "StudentUpdatedEvent"
  final val AdminStudentsDeleted = "StudentsDeletedEvent"
  final val AdminStudentSectionUpdated = "StudentSectionUpdatedEvent"
  final val AdminStudentEnabledEvent = "StudentEnabledEvent"
  final val AdminStudentDisabledEvent = "StudentDisabledEvent"
  final val AdminStudentMovedBetweenSchools = "StudentMovedBetweenSchools"
  final val AdminStudentGradeMovementEvent = "StudentMovedBetweenGrades"
  final val AdminStudentPromotedEvent = "StudentPromotedEvent"
  final val AdminTeacher = "TeacherCreatedEvent"
  final val AdminTeacherUpdate = "TeacherUpdatedEvent"
  final val AdminTeacherDisabledEvent = "TeacherDisabledEvent"
  final val AdminTeacherEnabledEvent = "TeacherEnabledEvent"
  final val AdminGuardianInvited = "GuardianInvitedEvent"
  final val AdminGuardianRegistered = "GuardianRegistered"
  final val AdminGuardianDeleted = "GuardianDeleted"
  final val AdminGuardianAssociations = "GuardianAssociationsUpdated"
  final val UserLogin = "LoggedInEvent"
  final val LearningSessionStarted = "LearningSessionStarted"
  final val LearningSessionFinished = "LearningSessionFinished"
  final val ExperienceStarted = "ExperienceStarted"
  final val ExperienceFinished = "ExperienceFinished"
  final val ExperienceSubmitted = "ExperienceSubmitted"
  final val LearningContentStarted = "ContentStartedEvent"
  final val LearningContentSkipped = "ContentSkippedEvent"
  final val LearningContentFinished = "ContentFinishedEvent"
  final val TotalScoreUpdatedEvent = "TotalScoreUpdatedEvent"
  final val PathwayActivityCompletedEvent = "PathwayActivityCompletedEvent"
  final val LevelCompletedEvent = "LevelCompletedEvent"
  final val LevelsRecommendedEvent = "LevelsRecommendedEvent"
  final val placementCompletionEvent = "PlacementCompletionEvent"
  final val placementTestCompletedEvent = "PlacementTestCompletedEvent"
  final val ExperienceDiscarded = "ExperienceDiscarded"
  final val SessionDeleted = "LearningSessionDeletedEvent"
  final val StudentDomainGradeChangedEvent = "StudentDomainGradeChangedEvent"
  final val StudentManualPlacementEvent = "StudentManualPlacementEvent"
  final val PathwayActivitiesAssignedEvent = "PathwayActivitiesAssignedEvent"
  final val PathwaysActivityUnAssignedEvent = "PathwaysActivityUnAssignedEvent"

  final val AdditionalResourcesAssignedEvent = "AdditionalResourceAssignedEvent"
  final val AdditionalResourceUnAssignedEvent = "AdditionalResourceUnAssignedEvent"

  final val CoreAdditionalResourcesAssignedEvent = "CoreAdditionalResourceAssignedEvent"

  final val UserCreated = "UserCreatedEvent"
  final val UserUpdated = "UserUpdatedEvent"
  final val UserDisabled = "UserDisabledEvent"
  final val UserEnabled = "UserEnabledEvent"
  final val UserMoved = "UserMovedBetweenSchoolEvent"
  final val UserUpdateEnableDisable = "UserUpdateEnableDisable"
  final val UserDeletedEvent = "UserDeletedEvent"

  final val RoleCreatedEvent = "RoleCreatedEvent"
  final val RoleUpdatedEvent = "RoleUpdatedEvent"
  final val RoleDeletedEvent = "RoleDeletedEvent"

  final val UserAvatarSelectedEvent = "UserAvatarSelectedEvent"
  final val UserAvatarUpdatedEvent = "UserAvatarUpdatedEvent"

  final val PracticeCreated = "PracticeCreatedEvent"
  final val PracticeSessionStarted = "PracticeSessionStartedEvent"
  final val PracticeSessionFinished = "PracticeSessionFinishedEvent"
  final val PracticeItemSessionStarted = "PracticeItemSessionStartedEvent"
  final val PracticeItemSessionFinished = "PracticeItemSessionFinishedEvent"
  final val PracticeItemContentSessionStarted = "PracticeItemContentSessionStartedEvent"
  final val PracticeItemContentSessionFinished = "PracticeItemContentSessionFinishedEvent"
  final val NoMloForSkillsFound = "NoMloForSkillsFound"

  final val KTGameSessionCreated = "GameSessionCreatedEvent"
  final val KTGameSessionStarted = "GameSessionStartedEvent"
  final val KTGameSessionFinished = "GameSessionFinishedEvent"
  final val KTGameQuestionSessionStarted = "GameQuestionSessionStartedEvent"
  final val KTGameQuestionSessionFinished = "GameQuestionSessionFinishedEvent"
  final val KTGameSessionCreationSkipped = "GameSessionCreationSkippedEvent"

  final val IncGameCreated = "InClassGameCreatedEvent"
  final val IncGameUpdated = "InClassGameUpdatedEvent"
  final val IncGameSessionStarted = "InClassGameSessionStartedEvent"
  final val IncGameSessionFinished = "InClassGameSessionFinishedEvent"
  final val IncGameSessionCreated = "InClassGameSessionCreatedEvent"
  final val IncGameSessionCancelled = "InClassGameSessionCancelledEvent"
  final val IncGameOutcome = "InClassGameOutcomeFinishedEvent"
  final val IncGameCreatedEventType = 1
  final val IncGameUpdatedEventType = 2
  final val IncGameUndefinedEventType = -1
  final val IncGameSessionStartedStatus = "IN_PROGRESS"
  final val IncGameSessionCompletedStatus = "COMPLETED"
  final val IncGameSessionCancelledStatus = "CANCELLED"
  final val IncGameSessionCreatedStatus = "OPEN"

  final val IncGameSessionStartedStatusVal = 1
  final val IncGameSessionCompletedStatusVal = 2
  final val IncGameSessionCancelledStatusVal = 3
  final val IncGameSessionCreatedStatusVal = 4
  final val IncGameSessionUndefinedStatusVal = -1

  final val IncGameOutcomeCompletedStatus = "COMPLETED"
  final val IncGameOutcomeSessionCancelled = "SESSION_CANCELLED"
  final val IncGameOutcomeSessionLeft = "SESSION_LEFT"
  final val IncGameOutcomeCompletedStatusVal = 1
  final val IncGameOutcomeSessionCancelledStatusVal = 2
  final val IncGameOutcomeSessionLeftStatusVal = 3
  final val IncGameOutcomeUndefinedStatusVal = -1

  final val AwardResource = "AwardResource"
  final val LearningObjectiveCreated = "learningObjective.create"
  final val LearningObjectiveUpdated = "LearningObjectiveUpdatedEvent"
  final val LearningObjectiveDeleted = "LearningObjectiveDeletedEvent"
  final val AdminLearningPath = "LevelCreatedEvent"
  final val AdminLearningPathUpdated = "LevelUpdatedEvent"
  final val AdminLearningPathDeleted = "LevelDeletedEvent"
  final val ConversationOccurred = "ConversationOccurred"
  final val TeacherSchoolMoved = "TeacherMovedBetweenSchools"
  final val AcademicYearStarted = "AcademicYearStarted"
  final val AcademicYearUpdated = "AcademicYearDateRangeChanged"
  final val AcademicYearRollOverCompleted = "AcademicYearRollOverCompleted"
  final val SchoolAcademicYearSwitched = "SchoolAcademicYearSwitched"
  final val ClassScheduleModifiedEvent = "ClassScheduleModifiedEvent"
  final val SectionScheduleModifiedEvent = "SectionScheduleModifiedEvent"
  final val StudentTagUpdatedEvent = "StudentTagUpdatedEvent"

  final val SkillCreated = "SkillCreatedEvent"
  final val SkillDeleted = "SkillDeletedEvent"
  final val SkillUpdatedEvent = "SkillUpdatedEvent"
  final val SkillLinkedEvent = "SkillLinkedEvent"
  final val SkillUnlinkedEvent = "SkillUnlinkedEvent"
  final val SkillCategoryLinkedEvent = "SkillCategoryLinkedEvent"
  final val SkillCategoryUnlinkedEvent = "SkillCategoryUnlinkedEvent"
  final val SkillLinkedEventV2 = "SkillLinkedEventV2"
  final val SkillUnlinkedEventV2 = "SkillUnlinkedEventV2"

  final val CategoryCreatedEvent = "CategoryCreatedEvent"
  final val CategoryUpdatedEvent = "CategoryUpdatedEvent"
  final val CategoryDeletedEvent = "CategoryDeletedEvent"

  final val OutcomeCreatedEvent = "OutcomeCreatedEvent"
  final val OutcomeUpdatedEvent = "OutcomeUpdatedEvent"
  final val OutcomeDeletedEvent = "OutcomeDeletedEvent"

  final val OutcomeSkillAttachedEvent = "OutcomeSkillAttachedEvent"
  final val OutcomeSkillDetachedEvent = "OutcomeSkillDetachedEvent"
  final val OutcomeCategoryAttachedEvent = "OutcomeCategoryAttachedEvent"
  final val OutcomeCategoryDetachedEvent = "OutcomeCategoryDetachedEvent"

  final val ContentCreatedEvent = "ContentCreatedEvent"
  final val ContentUpdatedEvent = "ContentUpdatedEvent"
  final val ContentDeletedEvent = "ContentDeletedEvent"
  final val ContentPublishedEvent = "ContentPublishedEvent"
  final val ContentOutcomeAttachedEvent = "ContentOutcomeAttachedEvent"
  final val ContentOutcomeDetachedEvent = "ContentOutcomeDetachedEvent"
  final val ContentSkillAttachedEvent = "ContentSkillAttachedEvent"
  final val ContentSkillDetachedEvent = "ContentSkillDetachedEvent"
  final val ClassContentAssignedEvent = "ClassContentAssignedEvent"
  final val ClassContentUnAssignedEvent = "ClassContentUnAssignedEvent"
  final val ClassLessonAssignedEvent = "ClassLessonAssignedEvent"
  final val ClassLessonUnAssignedEvent = "ClassLessonUnAssignedEvent"

  final val CurriculumCreatedEvent = "CurriculumCreatedEvent"
  final val CurriculumGradeCreatedEvent = "CurriculumGradeCreatedEvent"
  final val CurriculumSubjectCreatedEvent = "CurriculumSubjectCreatedEvent"
  final val CurriculumSubjectUpdatedEvent = "CurriculumSubjectUpdatedEvent"
  final val CurriculumSubjectDeletedEvent = "CurriculumSubjectDeletedEvent"

  final val LessonCreatedEvent = "LessonCreatedEvent"
  final val LessonMetadataUpdatedEvent = "LessonMetadataUpdatedEvent"
  final val LessonDeletedEvent = "LessonDeletedEvent"
  final val LessonPublishedEvent = "LessonPublishedEvent"
  final val LessonWorkflowStatusChangedEvent = "LessonWorkflowStatusChangedEvent"

  final val LessonActiveStatusVal = 1
  final val LessonDeletedStatusVal = 4
  final val LessonDraftActionStatus = "DRAFT"
  final val LessonReviewActionStatus = "REVIEW"
  final val LessonReworkActionStatus = "REWORK"
  final val LessonPublishedActionStatus = "PUBLISHED"
  final val LessonRepublishedActionStatus = "REPUBLISHED"
  final val LessonUndefinedActionStatusVal = -1
  final val LessonDraftActionStatusVal = 1
  final val LessonReviewActionStatusVal = 2
  final val LessonReworkActionStatusVal = 3
  final val LessonPublishedActionStatusVal = 4
  final val LessonRepublishedActionStatusVal = 5

  final val ActivityActiveStatusVal = 1
  final val ActivityDeletedStatusVal = 4
  final val ActivityDraftActionStatus = "DRAFT"
  final val ActivityReviewActionStatus = "REVIEW"
  final val ActivityReworkActionStatus = "REWORK"
  final val ActivityPublishedActionStatus = "PUBLISHED"
  final val ActivityRepublishedActionStatus = "REPUBLISHED"
  final val ActivityUndefinedActionStatusVal = -1
  final val ActivityDraftActionStatusVal = 1
  final val ActivityReviewActionStatusVal = 2
  final val ActivityReworkActionStatusVal = 3
  final val ActivityPublishedActionStatusVal = 4
  final val ActivityRepublishedActionStatusVal = 5

  final val LessonOutcomeAttachedEvent = "LessonOutcomeAttachedEvent"
  final val LessonOutcomeDetachedEvent = "LessonOutcomeDetachedEvent"
  final val LessonSkillLinkedEvent = "LessonSkillLinkedEvent"
  final val LessonSkillUnlinkedEvent = "LessonSkillUnlinkedEvent"
  final val LessonContentAttachedEvent = "LessonContentAttachedEvent"
  final val LessonContentDetachedEvent = "LessonContentDetachedEvent"
  final val LessonAssignmentAttachEvent = "LessonAssignmentAttachedEvent"
  final val LessonAssignmentDetachedEvent = "LessonAssignmentDetachedEvent"
  final val LessonAssessmentRuleAddedEvent = "LessonAssessmentRuleAddedEvent"
  final val LessonAssessmentRuleUpdatedEvent = "LessonAssessmentRuleUpdatedEvent"
  final val LessonAssessmentRuleRemovedEvent = "LessonAssessmentRuleRemovedEvent"

  final val ActivityTemplatePublishedEvent = "ActivityTemplatePublishedEvent"
  final val ActivityOutcomeAttachedEvent = "ActivityOutcomeAttachedEvent"
  final val ActivityOutcomeDetachedEvent = "ActivityOutcomeDetachedEvent"
  final val ActivityMetadataCreatedEvent = "ActivityMetadataCreatedEvent"
  final val ActivityMetadataUpdatedEvent = "ActivityMetadataUpdatedEvent"
  final val ActivityMetadataDeletedEvent = "ActivityMetadataDeletedEvent"
  final val ActivityWorkflowStatusChangedEvent = "ActivityWorkflowStatusChangedEvent"
  final val ActivityPublishedEvent = "ActivityPublishedEvent"

  final val SectionType = "sectionType"
  final val SectionTypeMainComp = 1
  final val SectionTypeSupportingComp = 2
  final val SectionTypeSideComp = 3
  final val MainComponents = "mainComponents"
  final val SupportingComponents = "supportingComponents"
  final val SideComponents = "sideComponents"
  final val Content = "CONTENT"
  final val ContentVal = 1
  final val Assignment = "ASSIGNMENT"
  final val AssignmentVal = 4
  final val Assessment = "ASSESSMENT"
  final val AssessmentVal = 5
  final val KeyTerm = "KEY_TERM"
  final val KeyTermVal = 6
  final val Undefined = -1

  final val LessonContentAssociationType = 1
  final val LessonOutcomeAssociationType = 2
  final val LessonSkillLinkType = 3
  final val LessonAssigmentAssociationType = 4
  final val LessonAssessmentRuleAssociationType = 5
  final val LessonAssociationActiveStatusVal = 1
  final val LessonAssociationInactiveStatusVal = 4
  final val LessonAssociationDetachedStatusVal = 0
  final val LessonAssociationAttachedStatusVal = 1
  final val LessonAssociationUndefinedStatusVal = -1
  final val Attached = 1
  final val Detached = 4
  final val OutcomeAssociationCategoryType = 1
  final val OutcomeAssociationSkillType = 2
  final val OutcomeAssociationDetachedStatusVal = 0
  final val OutcomeAssociationAttachedStatusVal = 1
  final val OutcomeAssociationUndefinedStatusVal = -1
  final val ClassScheduleDeletedStatusVal = 0
  final val ClassScheduleAddedStatusVal = 1
  final val ClassScheduleUndefinedStatusVal = -1

  final val ActivityOutcomeAssociationType = 2

  final val TagSchoolAssociationType = 1
  final val TagGradeAssociationType = 2
  final val TagUndefinedAssociationType = -1
  final val TagAssociationDetachedStatusVal = 0
  final val TagAssociationAttachedStatusVal = 1
  final val TagAssociationUndefinedStatusVal = -1

  final val ThemeCreatedEvent = "ThemeCreatedEvent"
  final val ThemeUpdatedEvent = "ThemeUpdatedEvent"
  final val ThemeDeletedEvent = "ThemeDeletedEvent"

  final val LessonTemplateCreatedEvent = "LessonTemplateCreatedEvent"
  final val LessonTemplateUpdatedEvent = "LessonTemplateUpdatedEvent"

  final val LessonFeedbackSubmitted = "LessonFeedbackSubmitted"
  final val LessonFeedbackCancelled = "LessonFeedbackCancelled"

  final val AssignmentCreatedEvent = "AssignmentCreatedEvent"
  final val AssignmentUpdatedEvent = "AssignmentUpdatedEvent"
  final val AssignmentInstanceCreatedEvent = "AssignmentInstanceCreatedEvent"
  final val AssignmentInstanceUpdatedEvent = "AssignmentInstanceUpdatedEvent"
  final val AssignmentDeletedEvent = "AssignmentDeletedEvent"
  final val AssignmentInstanceDeletedEvent = "AssignmentInstanceDeletedEvent"
  final val AssignmentSubmissionMutatedEvent = "AssignmentSubmissionMutatedEvent"
  final val AssignmentInstanceStudentsUpdatedEvent = "AssignmentInstanceStudentsUpdatedEvent"
  final val AssignmentResubmissionRequestedEvent = "AssignmentResubmissionRequestedEvent"

  final val TermWeekCreatedEvent = "TermCreatedEvent"
  final val InstructionalPlanPublishedEvent = "InstructionalPlanPublishedEvent"
  final val InstructionalPlanRePublishedEvent = "InstructionalPlanRePublishedEvent"
  final val InstructionalPlanDeletedEvent = "InstructionalPlanDeletedEvent"

  final val ClassCreatedEvent = "ClassCreatedEvent"
  final val ClassUpdatedEvent = "ClassUpdatedEvent"
  final val ClassDeletedEvent = "ClassDeletedEvent"
  final val StudentEnrolledInClassEvent = "StudentEnrolledInClassEvent"
  final val StudentUnenrolledFromClassEvent = "StudentUnenrolledFromClassEvent"
  final val ClassCategoryCreatedEvent = "ClassCategoryCreatedEvent"
  final val ClassCategoryUpdatedEvent = "ClassCategoryUpdatedEvent"
  final val ClassCategoryDeletedEvent = "ClassCategoryDeletedEvent"

  final val WeeklyGoalCreatedEvent = "WeeklyGoalCreated"
  final val WeeklyGoalUpdatedEvent = "WeeklyGoalUpdated"
  final val WeeklyGoalProgressEvent = "WeeklyGoalProgress"
  final val WeeklyGoalCompletedEvent = "WeeklyGoalCompleted"
  final val WeeklyGoalExpiredEvent = "WeeklyGoalExpired"
  final val WeeklyGoalStarEarnedEvent = "WeeklyGoalStarEarned"

  final val OrganizationCreatedEvent = "OrganizationCreatedEvent"
  final val OrganizationUpdatedEvent = "OrganizationUpdatedEvent"

  final val ContentRepositoryCreatedEvent = "ContentRepositoryCreatedEvent"
  final val ContentRepositoryUpdatedEvent = "ContentRepositoryUpdatedEvent"
  final val ContentRepositoryDeletedEvent = "ContentRepositoryDeletedEvent"
  final val ContentRepositoryMaterialAttachedEvent = "ContentRepositoryMaterialAttachedEvent"
  final val ContentRepositoryMaterialDetachedEvent = "ContentRepositoryMaterialDetachedEvent"

  final val DraftCourseCreatedEvent = "DraftCourseCreatedEvent"
  final val InReviewCourseEvent = "InReviewCourseEvent"
  final val CoursePublishedEvent = "CoursePublishedEvent"
  final val CourseSettingsUpdatedEvent = "CourseSettingsUpdatedEvent"
  final val CourseDeletedEvent = "CourseDeletedEvent"
  final val ContainerPublishedWithCourseEvent = "ContainerPublishedWithCourseEvent"
  final val ContainerAddedInCourseEvent = "ContainerAddedInCourseEvent"
  final val ContainerUpdatedInCourseEvent = "ContainerUpdatedInCourseEvent"
  final val ContainerDeletedFromCourseEvent = "ContainerDeletedFromCourseEvent"
  final val AbilityTestComponentEnabledEvent = "AbilityTestComponentEnabledEvent"
  final val AbilityTestComponentDisabledEvent = "AbilityTestComponentDisabledEvent"
  final val ActivityPlannedInAbilityTestComponentEvent = "ActivityPlannedInAbilityTestComponentEvent"
  final val ActivityUnPlannedInAbilityTestComponentEvent = "ActivityUnPlannedInAbilityTestComponentEvent"

  final val TestPlannedInAbilityTestComponentEvent = "TestPlannedInAbilityTestComponentEvent"
  final val TestUnPlannedInAbilityTestComponentEvent = "TestUnPlannedInAbilityTestComponentEvent"
  final val TestUpdatedInAbilityTestComponentEvent = "TestUpdatedInAbilityTestComponentEvent"
  final val AbilityTestComponentUpdatedEvent = "AbilityTestComponentUpdatedEvent"
  final val AdditionalResourceActivityPlannedEvent = "AdditionalResourceActivityPlannedEvent"
  final val AdditionalResourceActivityUpdatedEvent = "AdditionalResourceActivityUpdatedEvent"
  final val AdditionalResourceActivityUnplannedEvent = "AdditionalResourceActivityUnplannedEvent"
  final val DownloadableResourcePlannedEvent = "DownloadableResourcePlannedEvent"
  final val DownloadableResourceUpdatedEvent = "DownloadableResourceUpdatedEvent"
  final val DownloadableResourceUnplannedEvent = "DownloadableResourceUnplannedEvent"



  final val PacingGuideCreatedEvent = "PacingGuideCreatedEvent"
  final val PacingGuideUpdatedEvent = "PacingGuideUpdatedEvent"
  final val PacingGuideDeletedEvent = "PacingGuideDeletedEvent"

  final val CourseInstructionalPlanPublishedEvent = "InstructionalPlanPublishedEvent"
  final val CourseInstructionalPlanInReviewEvent = "InstructionalPlanInReviewEvent"
  final val CourseInstructionalPlanDeletedEvent = "InstructionalPlanDeletedEvent"

  final val DraftPathwayCreatedEvent = "DraftPathwayCreatedEvent"
  final val InReviewPathwayEvent = "InReviewPathwayEvent"
  final val PathwayPublishedEvent = "PathwayPublishedEvent"
  final val PathwayDetailsUpdatedEvent = "PathwayDetailsUpdatedEvent"
  final val PathwayDeletedEvent = "PathwayDeletedEvent"
  final val LevelPublishedWithPathwayEvent = "LevelPublishedWithPathwayEvent"

  final val ActivityPlannedInCourseEvent = "ActivityPlannedInCourseEvent"
  final val ActivityUpdatedInCourseEvent = "ActivityUpdatedInCourseEvent"
  final val ActivityUnPlannedInCourseEvent = "ActivityUnPlannedInCourseEvent"

  final val InterimCheckpointPublishedEvent = "InterimCheckpointPublishedEvent"
  final val InterimCheckpointUpdatedEvent = "InterimCheckpointUpdatedEvent"
  final val InterimCheckpointDeletedEvent = "InterimCheckpointDeletedEvent"

  final val ADTPlannedInCourseEvent = "ADTPlannedInCourseEvent"
  final val ADTUnPlannedInCourseEvent = "ADTUnPlannedInCourseEvent"
  final val ADTUpdatedInCourseEvent = "ADTUpdatedInCourseEvent"

  final val LevelAddedInPathwayEvent = "LevelAddedInPathwayEvent"
  final val LevelUpdatedInPathwayEvent = "LevelUpdatedInPathwayEvent"
  final val LevelDeletedFromPathwayEvent = "LevelDeletedFromPathwayEvent"

  final val StudentContentAssignedEvent = "ContentAssignedEvent"
  final val StudentContentUnAssignedEvent = "ContentUnAssignedEvent"
  final val LearnerLessonAssignedEvent = "LearnerLessonAssignedEvent"
  final val LearnerLessonUnAssignedEvent = "LearnerLessonUnAssignedEvent"

  final val QuestionCreatedEvent = "QuestionCreatedEvent"
  final val QuestionUpdatedEvent = "QuestionUpdatedEvent"

  final val PoolCreatedEvent = "PoolCreatedEvent"
  final val PoolUpdatedEvent = "PoolUpdatedEvent"
  final val QuestionsAddedToPoolEvent = "QuestionsAddedToPoolEvent"
  final val QuestionsRemovedFromPoolEvent = "QuestionsRemovedFromPoolEvent"

  final val TaggedEvent = "TaggedEvent"
  final val UntaggedEvent = "UntaggedEvent"

  final val TeacherFeedbackSentEvent = "TeacherFeedbackSentEvent"
  final val TeacherMessageSentEvent = "TeacherMessageSentEvent"
  final val GuardianFeedbackReadEvent = "GuardianFeedbackReadEvent"
  final val GuardianMessageSentEvent = "GuardianMessageSentEvent"
  final val GuardianJointActivityPendingEvent = "GuardianJointActivityPendingEvent"
  final val GuardianJointActivityStartedEvent = "GuardianJointActivityStartedEvent"
  final val GuardianJointActivityCompletedEvent = "GuardianJointActivityCompletedEvent"
  final val GuardianJointActivityRatedEvent = "GuardianJointActivityRatedEvent"
  final val TeacherMessageDeletedEvent = "TeacherMessageDeletedEvent"
  final val TeacherFeedbackDeletedEvent = "TeacherFeedbackDeletedEvent"

  final val ClassScheduleSlotAddedEvent = "ClassScheduleSlotAddedEvent"
  final val ClassScheduleSlotDeletedEvent = "ClassScheduleSlotDeletedEvent"
  final val ADTNextQuestionEvent = "NextQuestion"
  final val ADTStudentReportEvent = "StudentReport"
  final val AttemptThresholdCreatedEvent = "AttemptThresholdCreated"
  final val AttemptThresholdUpdatedEvent = "AttemptThresholdUpdated"

  //  CX Table Events
  final val UsersCreatedEvent = "UsersCreatedEvent"
  final val UsersUpdatedEvent = "UsersUpdatedEvent"
  final val UsersDeletedEvent = "UsersDeletedEvent"

  final val Schools_UsersCreatedEvent = "Schools_UsersCreatedEvent"
  final val Schools_UsersUpdatedEvent = "Schools_UsersUpdatedEvent"
  final val Schools_UsersDeletedEvent = "Schools_UsersDeletedEvent"

  final val RolesCreatedEvent = "RolesCreatedEvent"
  final val RolesUpdatedEvent = "RolesUpdatedEvent"
  final val RolesDeletedEvent = "RolesDeletedEvent"

  final val SchoolsCreatedEvent = "SchoolsCreatedEvent"
  final val SchoolsUpdatedEvent = "SchoolsUpdatedEvent"
  final val SchoolsDeletedEvent = "SchoolsDeletedEvent"

  final val MaturityCreatedEvent = "MaturityCreatedEvent"
  final val MaturityUpdatedEvent = "MaturityUpdatedEvent"
  final val MaturityDeletedEvent = "MaturityDeletedEvent"

  final val Maturity_IndicatorsCreatedEvent = "Maturity_IndicatorsCreatedEvent"
  final val Maturity_IndicatorsUpdatedEvent = "Maturity_IndicatorsUpdatedEvent"
  final val Maturity_IndicatorsDeletedEvent = "Maturity_IndicatorsDeletedEvent"

  final val ObservationsCreatedEvent = "ObservationsCreatedEvent"
  final val ObservationsUpdatedEvent = "ObservationsUpdatedEvent"
  final val ObservationsDeletedEvent = "ObservationsDeletedEvent"

  final val Observations_IndicatorsCreatedEvent = "Observations_IndicatorsCreatedEvent"
  final val Observations_IndicatorsUpdatedEvent = "Observations_IndicatorsUpdatedEvent"
  final val Observations_IndicatorsDeletedEvent = "Observations_IndicatorsDeletedEvent"

  final val TrainingsCreatedEvent = "TrainingsCreatedEvent"
  final val TrainingsUpdatedEvent = "TrainingsUpdatedEvent"
  final val TrainingsDeletedEvent = "TrainingsDeletedEvent"

  final val PLC_InfoCreatedEvent = "PLC_InfoCreatedEvent"
  final val PLC_InfoUpdatedEvent = "PLC_InfoUpdatedEvent"
  final val PLC_InfoDeletedEvent = "PLC_InfoDeletedEvent"

  final val User_logCreatedEvent = "User_logCreatedEvent"
  final val User_logUpdatedEvent = "User_logUpdatedEvent"
  final val User_logDeletedEvent = "User_logDeletedEvent"

  final val materialId = "materialId"
  final val materialType = "materialType"

  final val TeacherAnnouncementSentEvent = "TeacherAnnouncementSentEvent"
  final val PrincipalAnnouncementSentEvent = "PrincipalAnnouncementSentEvent"
  final val SuperintendentAnnouncementSentEvent = "SuperintendentAnnouncementSentEvent"
  final val AnnouncementDeletedEvent = "AnnouncementDeletedEvent"

  final val PathwayActiveStatusVal = 1
  final val PathwayInactiveStatusVal = 2

  final val CourseActiveStatusVal = 1
  final val CourseInactiveStatusVal = 2

  final val GuardianUserType = "GUARDIAN"
  final val TeacherUserType = "TEACHER"
  final val StudentUserType = "STUDENT"
  final val AdminUserType = "ADMIN"

  final val PathwayLevelActivityAssociationInactiveStatus = 2
  final val PathwayLevelActivityPlannedStatus = 1
  final val PathwayLevelActivityUnPlannedStatus = 4

  final val BadgesMetaDataUpdatedEvent = "BadgesMetaDataUpdatedEvent"
  final val StudentBadgeAwardedEvent = "StudentBadgeAwardedEvent"
  final val PathwayLeaderboardUpdatedEvent = "PathwayLeaderboardUpdatedEvent"
  final val CertificateAwardedEvent = "CertificateAwardedEvent"
  final val PathwayTargetMutatedEvent = "PathwayTargetMutatedEvent"
  final val StudentPathwayTargetMutatedEvent = "StudentPathwayTargetMutatedEvent"
  final val ActivitySettingsOpenPathEventEnabled = "ActivitySettingsOpenPathEnabledEvent"
  final val ActivitySettingsOpenPathEventDisabled = "ActivitySettingsOpenPathDisabledEvent"
  final val ActivitySettingsComponentVisibilityEventHide = "ActivitySettingsHideComponentEvent"
  final val ActivitySettingsComponentVisibilityEventShow = "ActivitySettingsShowComponentEvent"

  final val PurchaseTransactionEvent = "PurchaseTransactionEvent"
  final val ItemPurchasedEvent = "ItemPurchasedEvent"
  final val AvatarCreatedEvent = "AvatarCreatedEvent"
  final val AvatarUpdatedEvent = "AvatarUpdatedEvent"
  final val AvatarDeletedEvent = "AvatarDeletedEvent"
  final val AvatarLayerCreatedEvent = "AvatarLayerCreatedEvent"

  final val AcademicYearCreatedEvent = "AcademicYearCreatedEvent"
  final val AcademicYearUpdatedEvent = "AcademicYearUpdatedEvent"
  final val AcademicCalendarCreatedEvent = "AcademicCalendarCreatedEvent"
  final val AcademicCalendarUpdatedEvent = "AcademicCalendarUpdatedEvent"
  final val AcademicCalendarDeletedEvent = "AcademicCalendarDeletedEvent"

  final val EbookProgressEvent = "EbookProgressEvent"

  final val AlefGameChallengeProgressEvent = "AlefGameChallengeProgressEvent"
}
