package com.alefeducation.schema

import com.alefeducation.schema.activity._
import com.alefeducation.schema.activitysettings.{ComponentVisibilityEvent, OpenPathEvent}
import com.alefeducation.schema.admin._
import com.alefeducation.schema.adt.{AttemptThresholdCreated, NextQuestion, StudentReport}
import com.alefeducation.schema.announcement.{AnnouncementDeleted, PrincipalAnnouncement, SuperintendentAnnouncement, TeacherAnnouncement}
import com.alefeducation.schema.assignment._
import com.alefeducation.schema.auth.LoggedInEvent
import com.alefeducation.schema.badge._
import com.alefeducation.schema.ccl._
import com.alefeducation.schema.certificate.CertificateAwardedEvent
import com.alefeducation.schema.conversationOccurred.ConversationOccurred
import com.alefeducation.schema.course._
import com.alefeducation.schema.ebookprogress.EbookProgressEvent
import com.alefeducation.schema.guardian._
import com.alefeducation.schema.incgame.{IncGameEvent, IncGameOutcomeEvent, IncGameSessionEvent}
import com.alefeducation.schema.ktgame._
import com.alefeducation.schema.challengeGame._
import com.alefeducation.schema.leaderboard.LeaderboardEvent
import com.alefeducation.schema.lps._
import com.alefeducation.schema.marketplace._
import com.alefeducation.schema.newxapi.{ActivityObject, AgentObject, NewXAPIStatement}
import com.alefeducation.schema.notification.AwardResource
import com.alefeducation.schema.pathways._
import com.alefeducation.schema.pathwaytarget.{PathwayTargetMutatedEvent, StudentPathwayTargetMutatedEvent}
import com.alefeducation.schema.practice._
import com.alefeducation.schema.role.RoleEvent
import com.alefeducation.schema.studentAssignment._
import com.alefeducation.schema.teacherTest._
import com.alefeducation.schema.team.{TeamDeleted, TeamMembersUpdated, TeamMutated}
import com.alefeducation.schema.tutor.{TutorContextCreated, TutorConversation, TutorSession, TutorSuggestions, TutorCallToAction, TutorOnboarding, TutorAnalogous, TutorTranslation, TutorSimplification, TutorChallengeQuestions, TutorChallengeQuestionEvaluation, TutorSkipOnboarding}
import com.alefeducation.schema.user._
import com.alefeducation.schema.xapi.XAPIStatement
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe._

case class Headers(tenantId: String, eventType: String)
case class KafkaSchema(headers: Headers, body: String)

object Schema {

  val kafkaSchema: StructType = schema[KafkaSchema]

  val learningSessionStartedSchema: StructType = schema[LearningSessionStarted]

  val learningSessionFinishedSchema: StructType = schema[LearningSessionFinished]

  val experienceStartedSchema: StructType = schema[ExperienceStarted]

  val experienceFinishedSchema: StructType = schema[ExperienceFinished]

  val experienceDiscardedSchema: StructType = schema[ExperienceDiscarded]

  val sessionDeletedSchema: StructType = schema[LearningSessionDeletedEvent]

  val awardResourceSchema: StructType = schema[AwardResource]

  val sectionSchema: StructType = schema[SectionMutatedEvent]

  val sectionStateChangedSchema: StructType = schema[SectionStateChanged]

  val gradeSchema: StructType = schema[Grade]

  val gradeDeletedSchema: StructType = schema[GradeDeletedEvent]

  val schoolSchema: StructType = schema[School]

  val studentSchema: StructType = schema[StudentMutatedEvent]

  val studentsDeletedSchema: StructType = schema[StudentsDeletedEvent]

  val studentSectionUpdatedSchema: StructType = schema[StudentSectionUpdatedEvent]

  val studentStatusToggleSchema: StructType = schema[StudentToggleStatusEvent]

  val studentSchoolGradeMoveSchema: StructType = schema[StudentMovedBetweenSchoolsOrGrades]

  val studentPromotedSchema: StructType = schema[StudentPromotedEvent]

  val subjectSchema: StructType = schema[Subject]

  val teacherSchema: StructType = schema[Teacher]

  val guardianInvitedRegisteredEventSchema: StructType = schema[GuardianInvitedRegistered]

  val guardiansDeletedEventSchema: StructType = schema[GuardiansDeletedEvent]

  val guardianAssociationSchema: StructType = schema[GuardianAssociation]

  val guardianJointPendingActivitySchema: StructType = schema[GuardianJointActivityPendingEvent]

  val guardianJointStartedActivitySchema: StructType = schema[GuardianJointActivityStartedEvent]

  val guardianJointCompletedActivitySchema: StructType = schema[GuardianJointActivityCompletedEvent]

  val guardianJointRatedActivitySchema: StructType = schema[GuardianJointActivityRatedEvent]

  val userLoggedIn: StructType = schema[LoggedInEvent]

  val learningObjectiveSchema: StructType = schema[LearningObjective]

  val learningObjectiveUpdatedDeletedSchema: StructType = schema[LearningObjectiveUpdatedDeleted]

  val xapiSchema: StructType = schema[XAPIStatement]

  val agentObjectSchema: StructType = schema[AgentObject]

  val activityObjectSchema: StructType = schema[ActivityObject]

  val newXAPISchema: StructType = schema[NewXAPIStatement]

  val learningPathSchema: StructType = LearningPath.learningPathSchema

  val conversationOccurredSchema: StructType = schema[ConversationOccurred]

  val practiceCreatedSchema: StructType = schema[PracticeCreatedEvent]

  val practiceSessionStartedSchema: StructType = schema[PracticeSessionStarted]

  val practiceSessionFinishedSchema: StructType = schema[PracticeSessionFinished]

  val practiceItemSessionStartedSchema: StructType = schema[PracticeItemSessionStarted]

  val practiceItemSessionFinishedSchema: StructType = schema[PracticeItemSessionFinished]

  val practiceItemContentSessionStartedSchema: StructType = schema[PracticeItemContentSessionStarted]

  val practiceItemContentSessionFinishedSchema: StructType = schema[PracticeItemContentSessionFinished]

  val noMloForSkillsFoundSchema: StructType = schema[NoMloForSkillsFound]

  val userSchema: StructType = schema[User]

  val userMovedSchema: StructType = schema[UserMovedBetweenSchools]

  val userDeletedSchema: StructType = schema[UserDeleted]

  val roleSchema: StructType = schema[RoleEvent]

  val userAvatarSchema: StructType = schema[UserAvatar]

  val teacherSchoolMovedSchema: StructType = schema[TeacherMovedBetweenSchools]

  val academicYearMutated: StructType = schema[AcademicYearMutatedEvent]

  val academicYearRollOverSchema: StructType = schema[AcademicYearRollOverEvent]

  val schoolAcademicYearSwitchedSchema: StructType = schema[SchoolAcademicYearSwitched]

  val ktGameSessionCreatedSchema: StructType = schema[GameSessionCreatedEvent]

  val ktGameSessionStartedSchema: StructType = schema[GameSessionStartedEvent]

  val ktGameSessionFinishedSchema: StructType = schema[GameSessionFinishedEvent]

  val ktGameQuestionSessionStartedSchema: StructType = schema[GameQuestionSessionStartedEvent]

  val ktGameQuestionSessionFinishedSchema: StructType = schema[GameQuestionSessionFinishedEvent]

  val ktGameSessionCreationSkippedSchema: StructType = schema[GameSessionCreationSkippedEvent]

  val incGameSchema: StructType = schema[IncGameEvent]

  val incGameSessionSchema: StructType = schema[IncGameSessionEvent]

  val incGameOutcomeSchema: StructType = schema[IncGameOutcomeEvent]

  val studentTagUpdatedSchema: StructType = schema[StudentTagUpdatedEvent]

  val cclSkillMutatedSchema: StructType = schema[CCLSkillMutated]
  val cclSkillDeletedSchema: StructType = schema[CCLSkillDeleted]
  val cclSkillLinkToggleSchema: StructType = schema[CCLSkillLinkToggle]
  val cclSkillLinkToggleV2Schema: StructType = schema[CCLSkillLinkToggleV2]
  val cclSkillCategoryLinkToggleSchema: StructType = schema[CCLSkillCategoryLinkToggle]
  val categoryCreatedSchema: StructType = schema[CategoryCreated]
  val categoryUpdatedSchema: StructType = schema[CategoryUpdated]
  val categoryDeletedSchema: StructType = schema[CategoryDeleted]

  val outcomeCreatedSchema: StructType = schema[OutcomeCreated]
  val outcomeUpdatedSchema: StructType = schema[OutcomeUpdatedEvent]
  val outcomeDeletedSchema: StructType = schema[OutcomeDeletedEvent]
  val outcomeSkillLinkSchema: StructType = schema[OutcomeSkillAttachEvent]
  val outcomeCategoryLinkSchema: StructType = schema[OutcomeCategoryAttachEvent]

  val contentCreatedSchema: StructType = schema[ContentCreatedEvent]
  val contentUpdatedSchema: StructType = schema[ContentUpdatedEvent]
  val contentDeletedSchema: StructType = schema[ContentDeletedEvent]
  val contentPublishedSchema: StructType = schema[ContentPublishedEvent]
  val contentOutcomeAttachSchema: StructType = schema[ContentOutcomeAttachEvent]
  val contentSkillAttachedDetachedSchema: StructType = schema[ContentSkillAttachedDetachedEvent]
  val contentClassAssignedUnAssignedSchema: StructType = schema[ClassContentAssignedUnAssignedEvent]
  val lessonClassAssignedUnAssignedSchema: StructType = schema[ClassLessonAssignedUnAssignedEvent]

  val CurriculumCreatedSchema: StructType = schema[CurriculumCreatedEvent]
  val CurriculumGradeCreatedSchema: StructType = schema[CurriculumGradeCreatedEvent]
  val CurriculumSubjectCreatedSchema: StructType = schema[CurriculumSubjectCreatedEvent]
  val CurriculumSubjectUpdatedSchema: StructType = schema[CurriculumSubjectUpdatedEvent]
  val CurriculumSubjectDeletedSchema: StructType = schema[CurriculumSubjectDeletedEvent]

  val lessonMutatedSchema: StructType = schema[LessonMutatedEvent]
  val lessonDeletedSchema: StructType = schema[LessonDeletedEvent]
  val lessonPublishedSchema: StructType = schema[LessonPublishedEvent]
  val lessonWorkflowStatusChangedSchema: StructType = schema[LessonWorkflowStatusChangedEvent]

  val lessonOutcomeAttachSchema: StructType = schema[LessonOutcomeAttachEvent]
  val lessonSkillLinkSchema: StructType = schema[LessonSkillLinkEvent]
  val lessonContentAttachSchema: StructType = schema[LessonContentAttachEvent]

  val lessonFeedback: StructType = schema[LessonFeedback]

  val assignmentMutatedSchema: StructType = schema[AssignmentMutatedEvent]
  val assignmentInstanceMutatedSchema: StructType = schema[AssignmentInstanceMutatedEvent]
  val assignmentDeletedSchema: StructType = schema[AssignmentDeletedEvent]
  val assignmentInstanceDeletedSchema: StructType = schema[AssignmentInstanceDeletedEvent]
  val assignmentSubmittedSchema: StructType = schema[AssignmentSubmittedEvent]
  val assignmentInstanceStudentsUpdatedSchema: StructType = schema[AssignmentInstanceStudentsUpdatedSchema]
  val AssignmentResubmissionRequestedEventSchema: StructType = schema[AssignmentResubmissionRequestedEvent]

  val termWeekSchema: StructType = schema[TermCreated]
  val instructionalPlanPublishedSchema: StructType = schema[InstructionalPlanPublished]
  val instructionalPlanRePublishedSchema: StructType = schema[InstructionalPlanRePublished]
  val instructionalPlanDeletedSchema: StructType = schema[InstructionalPlanDeleted]

  val classModifiedSchema: StructType = schema[ClassMutated]
  val classDeletedSchema: StructType = schema[ClassDeleted]
  val studentClassAssociationSchema: StructType = schema[StudentClassEnrollmentToggle]
  val classCategorySchema: StructType = schema[ClassCategory]

  val studentContentAssignmentSchema: StructType = schema[StudentContentAssignment]
  val studentLessonAssignmentSchema: StructType = schema[StudentLessonAssignment]

  val teamMutatedSchema: StructType = schema[TeamMutated]
  val teamDeletedSchema: StructType = schema[TeamDeleted]
  val teamMembersUpdatedSchema: StructType = schema[TeamMembersUpdated]

  val themeCreatedSchema: StructType = schema[ThemeCreated]
  val themeUpdatedSchema: StructType = schema[ThemeUpdated]
  val themeDeletedSchema: StructType = schema[ThemeDeleted]

  val templateCreatedSchema: StructType = schema[LessonTemplateCreated]
  val templateUpdatedSchema: StructType = schema[LessonTemplateUpdated]

  val questionMutatedSchema: StructType = schema[QuestionModifiedEvent]

  val questionPoolSchema: StructType = schema[QuestionPoolMutatedEvent]
  val questionPoolAssociationSchema: StructType = schema[QuestionsPoolAssociationEvent]

  val tagEventSchema: StructType = schema[TagEvent]

  val classScheduleSlotSchema: StructType = schema[ClassScheduleSlot]

  val teacherFeedbackSentSchema: StructType = schema[TeacherFeedbackSentEvent]
  val teacherMessageSentSchema: StructType = schema[TeacherMessageSentEvent]
  val guardianFeedbackReadSchema: StructType = schema[GuardianFeedbackReadEvent]
  val guardianMessageSentSchema: StructType = schema[GuardianMessageSentEvent]
  val teacherMessageDeletedSchema: StructType = schema[TeacherMessageDeletedEvent]
  val teacherFeedbackDeletedSchema: StructType = schema[TeacherFeedbackDeletedEvent]
  val lessonAssignmentAssociationSchema: StructType = schema[LessonAssignmentAssociationEvent]
  val lessonAssessmentRuleAssociationSchema: StructType = schema[LessonAssessmentRuleAssociationEvent]
  val experienceSubmittedSchema: StructType = schema[ExperienceSubmitted]
  val learningContentStartFinishSchema: StructType = schema[LearningContentStartSkipped]
  val learningContentFinsihedSchema: StructType = schema[LearningContentFinished]
  val totalScoreUpdatedSchema: StructType = schema[TotalScoreUpdatedEvent]
  val adtNextQuestionSchema: StructType = schema[NextQuestion]
  val adtStudentReportSchema: StructType = schema[StudentReport]
  val adtAttemptThresholdCreatedSchema: StructType = schema[AttemptThresholdCreated]
  val adtAttemptThresholdUpdatedSchema: StructType = schema[AttemptThresholdCreated]
  val schoolStatusToggleSchema: StructType = schema[SchoolStatusToggleEvent]
  val pathwayActivityCompletedSchema: StructType = schema[PathwayActivityCompletedEvent]
  val levelCompletedSchema: StructType = schema[LevelCompletedEvent]
  val levelsRecommendedSchema: StructType = schema[LevelsRecommendedEvent]
  val placementCompletionSchema: StructType = schema[PlacementCompletionEvent]
  val placementTestCompletedSchema: StructType = schema[PlacementTestCompletedEvent]

  val scoreBreakDownSchema: ArrayType = schemaArr[Seq[ScoreBreakdown]]

  val activityTemplateSchema: StructType = schema[ActivityTemplateEvent]
  val activityOutcomeSchema: StructType = schema[ActivityOutcomeEvent]

  val activityMetadataSchema: StructType = schema[ActivityMetadataEvent]
  val activityMetadataDeletedSchema: StructType = schema[ActivityMetadataDeletedEvent]
  val activityWorkflowStatusChangedSchema: StructType = schema[ActivityWorkflowStatusChangedEvent]

  val weeklyGoalSchema: StructType = schema[WeeklyGoalMutatedEvent]
  val weeklyGoalStarSchema: StructType = schema[WeeklyGoalStarEvent]
  val weeklyGoalProgressEventSchema: StructType = schema[WeeklyProgressEvent]

  val organizationSchema = schema[Organization]
  val contentRepositoryCreatedSchema = schema[ContentRepositoryCreated]
  val contentRepositoryDeletedSchema = schema[ContentRepositoryDeleted]
  val contentRepositoryAttachedSchema = schema[ContentRepositoryMaterialAttachedEvent]
  val contentRepositoryDetachedSchema = schema[ContentRepositoryMaterialDetachedEvent]

  val pathwayDraftCreatedSchema: StructType = schema[PathwayDraftCreated]
  val pathwayInReviewSchema: StructType = schema[PathwayInReviewSchema]
  val pathwayPublishedSchema: StructType = schema[PathwayPublishedSchema]
  val pathwayDetailsUpdatedSchema: StructType = schema[PathwayDetailsUpdatedSchema]
  val pathwayDeletedSchema: StructType = schema[PathwayDeletedSchema]
  val levelPublishedWithPathwaySchema: StructType = schema[LevelPublishedWithPathwaySchema]

  val courseDraftCreatedSchema: StructType = schema[DraftCourseCreated]
  val courseInReviewSchema: StructType = schema[CourseInReviewSchema]
  val coursePublishedSchema: StructType = schema[CoursePublishedSchema]
  val courseSettingsUpdatedSchema: StructType = schema[CourseSettingsUpdatedSchema]
  val courseDeletedSchema: StructType = schema[CourseDeletedSchema]
  val containerPublishedWithCourseSchema: StructType = schema[ContainerPublishedWithCourseSchema]

  val activityPlannedInCourseSchema: StructType = schema[ActivityPlannedInCourseSchema]
  val activityUnPlannedInCourseSchema: StructType = schema[ActivityUnPlannedInCourseSchema]
  val activityUpdatedInCourseSchema: StructType = schema[ActivityUpdatedInCourseSchema]

  val interimCheckpointPublishedSchema: StructType = schema[InterimCheckpointPublishedSchema]
  val interimCheckpointUpdatedSchema: StructType = schema[InterimCheckpointUpdatedSchema]
  val interimCheckpointDeletedSchema: StructType = schema[InterimCheckpointDeletedEvent]

  val containerAddedInCourseSchema: StructType = schema[ContainerAddedInCourseSchema]
  val containerUpdatedInCourseSchema: StructType = schema[ContainerAddedInCourseSchema]
  val containerDeletedFromCourseSchema: StructType = schema[ContainerDeletedFromCourseSchema]

  val abilityTestComponentEnabledSchema: StructType = schema[AbilityTestComponentEnabledSchema]
  val abilityTestComponentDisabledSchema: StructType = schema[AbilityTestComponentDisabledSchema]
  val activityPlannedInAbilityTestComponentSchema: StructType = schema[ActivityPlannedInAbilityTestComponentSchema]
  val activityUnPlannedInAbilityTestComponentSchema: StructType = schema[ActivityUnPlannedInAbilityTestComponentSchema]
  val abilityTestComponentUpdatedSchema: StructType = schema[AbilityTestComponentUpdatedSchema]
  val testPlannedInAbilityTestComponentSchema: StructType = TestPlannedInAbilityTestComponentSchema.testPlannedInAbilityTestComponentSchema
  val testUnPlannedInAbilityTestComponentSchema: StructType = TestPlannedInAbilityTestComponentSchema.testPlannedInAbilityTestComponentSchema
  val testUpdatedInAbilityTestComponentSchema: StructType = TestPlannedInAbilityTestComponentSchema.testPlannedInAbilityTestComponentSchema

  val additionalResourceActivityPlannedSchema: StructType = schema[AdditionalResourceActivityPlannedSchema]
  val additionalResourceActivityUnplannedSchema: StructType = schema[AdditionalResourceActivityUnplannedSchema]
  val additionalResourceActivityUpdatedSchema: StructType = schema[AdditionalResourceActivityUpdatedSchema]
  val downloadableResourcePlannedSchema: StructType = schema[DownloadableResourcePlannedSchema]
  val downloadableResourceUnplannedSchema: StructType = schema[DownloadableResourceUnplannedSchema]
  val downloadableResourceUpdatedSchema: StructType = schema[DownloadableResourceUpdatedSchema]

  val pacingGuideCreatedEventSchema: StructType = schema[PacingGuideCreatedEventSchema]
  val pacingGuideUpdatedEventSchema: StructType = schema[PacingGuideUpdatedEventSchema]
  val pacingGuideDeletedEventSchema: StructType = schema[PacingGuideDeletedEventSchema]

  val courseInstructionalPlanInReviewEventSchema: StructType = schema[InstructionalPlanInReviewEvent]
  val courseInstructionalPlanPublishedEventSchema: StructType = schema[InstructionalPlanPublishedEvent]
  val courseInstructionalPlanDeletedEventSchema: StructType = schema[InstructionalPlanDeletedEvent]

  val levelAddedInPathwaySchema: StructType = schema[LevelAddedInPathwaySchema]
  val levelUpdatedInPathwaySchema: StructType = schema[LevelAddedInPathwaySchema]
  val levelDeletedFromPathwaySchema: StructType = schema[LevelDeletedFromPathwaySchema]

  val adtPlannedInCourseSchema: StructType = schema[ADTPlannedInCourseSchema]
  val adtUnPlannedInCourseSchema: StructType = schema[ADTUnPlannedInCourseSchema]

  val adtUpdatedCourseSchema: StructType = schema[ADTPlannedInCourseSchema]

  val teacherAnnouncementSchema: StructType = schema[TeacherAnnouncement]
  val principalAnnouncementSchema: StructType = schema[PrincipalAnnouncement]
  val superintendentAnnouncementSchema: StructType = schema[SuperintendentAnnouncement]
  val announcementDeletedSchema: StructType = schema[AnnouncementDeleted]

  val badgeUpdatedSchema: StructType = schema[BadgeUpdated]
  val studentBadgeAwardedSchema: StructType = schema[StudentBadgeAwarded]
  val leaderboardEventSchema: StructType = schema[LeaderboardEvent]
  val certificateAwardedEventSchema: StructType = schema[CertificateAwardedEvent]
  val pathwayTargetMutatedEventSchema: StructType = schema[PathwayTargetMutatedEvent]
  val studentPathwayTargetMutatedSchema: StructType = schema[StudentPathwayTargetMutatedEvent]

  val itemPurchasedEventSchema: StructType = schema[ItemPurchasedEvent]
  val purchaseTransactionEventSchema: StructType = schema[PurchaseTransactionEvent]
  val avatarCreatedEventSchema: StructType = schema[AvatarCreatedEvent]
  val avatarDeletedEventSchema: StructType = schema[AvatarDeletedEvent]
  val avatarUpdatedEventSchema: StructType = schema[AvatarUpdatedEvent]
  val avatarLayerCreatedEventSchema: StructType = schema[AvatarLayerCreatedEvent]

  val tutorContextSchema: StructType = schema[TutorContextCreated]
  val tutorSessionSchema: StructType = schema[TutorSession]
  val tutorConversationSchema: StructType = schema[TutorConversation]
  val tutorSuggestionsSchema: StructType = schema[TutorSuggestions]
  val tutorOnboardingSchema: StructType = schema[TutorOnboarding]
  val tutorCallToActionSchema: StructType = schema[TutorCallToAction]

  val tutorAnalogousSchema: StructType = schema[TutorAnalogous]
  val tutorTranslationSchema: StructType = schema[TutorTranslation]
  val tutorSimplificationSchema: StructType = schema[TutorSimplification]
  val tutorChallengeQuestionsSchema: StructType = schema[TutorChallengeQuestions]
  val tutorChallengeQuestionEvaluationSchema: StructType = schema[TutorChallengeQuestionEvaluation]

  val heartbeatSchema: StructType = schema[Heartbeat]

  val studentDomainGradeChangedSchema = schema[StudentDomainGradeChangedEvent]
  val studentManualPlacementSchema = schema[StudentManualPlacementEvent]

  val PathwayActivitiesAssignedSchema = schema[PathwayActivitiesAssignedEvent]
  val PathwaysActivityUnAssignedSchema = schema[PathwaysActivityUnAssignedEvent]
  val activitySettingsOpenPathEventSchema: StructType = schema[OpenPathEvent]
  val activitySettingsComponentVisibilityEventSchema: StructType = schema[ComponentVisibilityEvent]

  val AdditionalResourcesAssignedSchema = schema[AdditionalResourcesAssignedEvent]
  val AdditionalResourceUnAssignedSchema = schema[AdditionalResourceUnAssignedEvent]

  val CoreAdditionalResourcesAssignedSchema = schema[CoreAdditionalResourcesAssignedEvent]

  val testBlueprintCreatedIntegrationEventSchema: StructType = schema[TestBlueprintCreatedIntegrationEvent]
  val testBlueprintContextUpdatedIntegrationEventSchema: StructType = schema[TestBlueprintContextUpdatedIntegrationEvent]
  val testBlueprintGuidanceUpdatedIntegrationEventSchema: StructType = schema[TestBlueprintGuidanceUpdatedIntegrationEvent]
  val testBlueprintMadeReadyIntegrationEventSchema: StructType = schema[TestBlueprintMadeReadyIntegrationEvent]
  val testBlueprintPublishedIntegrationEventSchema: StructType = schema[TestBlueprintPublishedIntegrationEvent]
  val testBlueprintDiscardedIntegrationEventSchema: StructType = schema[TestBlueprintDiscardedIntegrationEvent]
  val testBlueprintArchivedIntegrationEventSchema: StructType = schema[TestBlueprintArchivedIntegrationEvent]

  val testCreatedIntegrationEventSchema: StructType = schema[TestCreatedIntegrationEvent]
  val testContextUpdatedIntegrationEventSchema: StructType = schema[TestContextUpdatedIntegrationEvent]
  val itemReplacedIntegrationEventSchema: StructType = schema[ItemReplacedIntegrationEvent]
  val itemReorderedIntegrationEventSchema: StructType = schema[ItemReorderedIntegrationEvent]
  val testItemsRegeneratedIntegrationEventSchema: StructType = schema[TestItemsRegeneratedIntegrationEvent]
  val testPublishedIntegrationEventSchema: StructType = schema[TestPublishedIntegrationEvent]
  val testDiscardedIntegrationEventSchema: StructType = schema[TestDiscardedIntegrationEvent]
  val testArchivedIntegrationEventSchema: StructType = schema[TestArchivedIntegrationEvent]

  val testDeliveryCreatedIntegrationEventSchema: StructType = schema[TestDeliveryCreatedIntegrationEvent]
  val testDeliveryStartedIntegrationEventSchema: StructType = schema[TestDeliveryStartedIntegrationEvent]
  val testDeliveryArchivedIntegrationEventSchema: StructType = schema[TestDeliveryArchivedIntegrationEvent]
  val testDeliveryCandidateUpdatedIntegrationEventSchema: StructType = schema[TestDeliveryCandidateUpdatedIntegrationEvent]

  val candidateSessionRecorderMadeInProgressIntegrationEventSchema: StructType = schema[CandidateSessionRecorderMadeInProgressIntegrationEvent]
  val candidateSessionRecorderMadeCompletedIntegrationEventSchema: StructType = schema[CandidateSessionRecorderMadeCompletedIntegrationEvent]
  val candidateSessionRecorderArchivedIntegrationEventSchema: StructType = schema[CandidateSessionRecorderArchivedIntegrationEvent]

  val academicYearMutatedEventSchema: StructType = schema[AcademicYearMutatedEventSchema]
  val academicCalendarMutatedEventSchema: StructType = AcademicCalendarMutatedEventSchema.academicCalendarMutatedEventSchema
  val ebookProgressEventSchema: StructType = schema[EbookProgressEvent]

  val challengeGameProgressEventSchema: StructType = schema[AlefGameChallengeProgressEvent]

  def schema[T: TypeTag]: StructType = schemaFor[T].dataType.asInstanceOf[StructType]

  def schemaArr[T: TypeTag]: ArrayType = schemaFor[T].dataType.asInstanceOf[ArrayType]

}
