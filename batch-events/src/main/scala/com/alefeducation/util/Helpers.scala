package com.alefeducation.util

import scala.collection.immutable.Map

object Helpers {
  lazy val MinusOne: Int = -1
  lazy val MinusOneDecimal = -1.0
  lazy val DateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS"
  lazy val DefaultStringValue = "n/a"
  lazy val DefaultDay = "1970-01-01"
  lazy val DefaultTime = "1970-01-01 00:00:00.000"
  lazy val DefaultScore = -1
  lazy val DefaultStars = -1
  lazy val DEFAULT_ID = "DEFAULT_ID"
  lazy val DefaultRetry = false
  lazy val DefaultOutsideOfSchool = false
  lazy val StartingIndexConversationOccurred = 0
  lazy val EndIndexConversationOccurred = 2046
  lazy val StartingIndexTimestampLocal = 0
  lazy val EndIndexTimestampLocal = 100
  lazy val StartingIndex = 0
  lazy val EndIndexTopic = 200
  lazy val ZeroArraySize = 0
  val DefaultTimeOutSeconds = 30 * 60

  val practiceSessionType = 1
  val practiceItemSessionType = 2
  val practiceItemContentSessionType = 3

  val EventType = "eventType"

  val NULL = null
  val DEFAULT_DATE = "1970-01-01 00:00:00.0"

  val GuardianInvitationSent = 2
  val GuardianRegistered = 2

  val Guardian = "GUARDIAN"
  val Teacher = "TEACHER"
  val Student = "STUDENT"

  val Activity = "Activity"
  val Agent = "Agent"

  val ActiveEnabled = 1
  val Inactive = 2
  val Disabled = 3
  val Deleted = 4

  val Enroll = "Enroll"
  val UnEnroll = "UnEnroll"
  val EnrollStatus = 1
  val UnEnrollStatus = 2;


  lazy val Insert = "INSERT"
  lazy val Update = "UPDATE"
  lazy val Delete = "DELETE"
  lazy val InsertWithHistory = "INSERTWITHHISTORY"
  val AwardResourceSource = "award-resource-source"

  val AwardResourceAggregationService = "award-resource-aggregation"
  val ParquetAwardResourceSource = "parquet-award-resource-source"
  val RedshiftAwardCategoriesSource = "redshift-award-categories-source"

  val ParquetContentCreatedSource = "parquet-content-created-source"
  val ParquetContentUpdatedSource = "parquet-content-updated-source"
  val ParquetContentPublishedSource = "parquet-content-published-source"
  val ParquetContentDeletedSource = "parquet-content-deleted-source"

  val ParquetCurriculumCreatedSource = "parquet-curriculum-created-source"
  val ParquetCurriculumGradeCreatedSource = "parquet-curriculum-grade-created-source"
  val ParquetCurriculumSubjectCreatedSource = "parquet-curriculum-subject-created-source"
  val ParquetCurriculumSubjectUpdatedSource = "parquet-curriculum-subject-updated-source"
  val ParquetCurriculumSubjectDeletedSource = "parquet-curriculum-subject-deleted-source"

  val ParquetContentOutcomeSource = "parquet-content-outcome-source"
  val ParquetContentSkillSource = "parquet-content-skill-source"
  val ParquetStudentContentSource = "parquet-content-student-source"
  val ParquetContentClassSource = "parquet-content-class-source"
  val ParquetLessonStudentSource = "parquet-lesson-student-source"
  val ParquetLessonClassSource = "parquet-lesson-class-source"

  val ParquetOutcomeCategorySource, ParquetOutcomeCategorySink = "parquet-outcome-category-source"
  val ParquetOutcomeSkillSource, ParquetOutcomeSkillSink = "parquet-outcome-skill-source"

  val ParquetTagSource, ParquetTagSink = "parquet-tag-source"
  val ParquetTagStagingSource, ParquetTagStagingSink = "parquet-tag-staging-source"

  val ParquetSchoolSource = "parquet-school-source"
  val ParquetSchoolStatusSource = "parquet-school-status-toggle-source"
  val ParquetGradeSource, ParquetGradeSink = "parquet-grade-source"
  val ParquetGradeDeletedSource, ParquetGradeDeletedSink = "parquet-grade-deleted-source"
  val ParquetSectionSource = "parquet-section-source"
  val ParquetSectionStateChangedSource = "parquet-section-state-changed-source"
  val ParquetLearningPathSource = "parquet-learning-path-source"
  val ParquetStudentSource = "parquet-student-source"
  val ParquetStudentsDeletedSource = "parquet-students-deleted-source"
  val ParquetStudentSectionUpdatedSource = "parquet-student-section-updated-source"
  val ParquetStudentEnableDisableSource = "parquet-student-enable-disable-source"
  val ParquetStudentSchoolOrGradeMoveSource = "parquet-student-school-grade-move-source"
  val ParquetStudentPromotedSource = "parquet-student-promoted-source"
  val ParquetStudentTagUpdatedSource = "parquet-student-tag-updated-source"
  val ParquetTeacherSource = "parquet-teacher-source"
  val ParquetTeacherSubjectAssignedSource = "parquet-teacher-subject-assignment-changed-source"
  val ParquetTeacherSchoolMovedSource = "parquet-teacher-school-moved-source"
  val ParquetGuardianAssociationsSource = "parquet-guardian-associations-source"
  val DeltaGuardianSource = "delta-guardian-dim"
  val ParquetGuardianDeletedSource = "parquet-guardian-deleted-source"
  val ParquetGuardianSource = "parquet-guardian-source"
  val ParquetUserSource = "parquet-user-source"
  val ParquetAdminSchoolMovedSource = "parquet-admin-school-moved-source"
  val ParquetUserDeletedSource = "parquet-user-deleted-source"
  val ParquetConversationOccurredSource, ParquetConversationOccurredSink = "parquet-conversation-occurred-source"
  val ParquetPracticeSource = "parquet-practice-source"
  val NoSkillContentSource = "parquet-no-skill-content-source"

  val PracticeSessionStarted = "parquet-practice-session-started-source"
  val PracticeSessionFinished = "parquet-practice-session-finished-source"
  val PracticeItemSessionStarted = "parquet-practice-item-session-started-source"
  val PracticeItemSessionFinished = "parquet-practice-item-session-finished-source"
  val PracticeItemContentSessionStarted = "parquet-practice-item-content-session-started-source"
  val PracticeItemContentSessionFinished = "parquet-practice-item-content-session-finished-source"
  val PracticeDeltaStaging = "parquet-practice-delta-staging-source"
  val PracticeItemDeltaStaging = "parquet-practice-item-delta-staging-source"
  val PracticeItemContentDeltaStaging = "parquet-practice-item-content-delta-staging-source"

  val AssignmentInstanceMutatedParquetSource = "parquet-assignment-instance-mutated-source"
  val AssignmentInstanceDeletedParquetSource = "parquet-assignment-instance-deleted-source"
  val AssignmentInstanceDeltaSink = "delta-assignment-instance-sink"
  val AssignmentInstanceRedshiftSink = "redshift-assignment-instance-sink"
  val AssignmentMutatedParquetSource = "parquet-assignment-mutated-source"
  val AssignmentDeletedParquetSource = "parquet-assignment-deleted-source"
  val AlefAssignmentMutatedParquetSource = "parquet-ccl-assignment-mutated-source"
  val AlefAssignmentDeletedParquetSource = "parquet-ccl-assignment-deleted-source"
  val AssignmentDeltaSink = "delta-assignment-sink"
  val AssignmentRedshiftSink = "redshift-assignment-sink"

  val AssignmentInstanceStudentParquetSource = "parquet-assignment-instance-student-source"
  val AssignmentInstanceStudentDeltaSink = "delta-assignment-instance-student-sink"
  val AssignmentInstanceStudentRedshiftSink = "redshift-assignment-instance-student-sink"

  val SubjectParquetSource = "parquet-subject-source"

  val ParquetLessonFeedbackSource = "parquet-lesson-feedback-source"
  val RedshiftLessonFeedbackSink = "redshift-lesson-feedback-sink"
  val DeltaLessonFeedbackSink = "delta-lesson-feedback-sink"

  val IncGameSource = "parquet-in-class-game-source"
  val IncGameSessionSource = "parquet-in-class-game-session-source"
  val IncGameSessionDeltaStagingSource = "parquet-in-class-game-session-delta-staging-source"
  val IncGameOutcomeSource = "parquet-in-class-game-outcome-source"

  val LessonFeedbackService = "lesson-feedback"

  val NoSkillContentService: String = "no-skill-content"

  val CsvCurioStudentSource = "csv-curio-student-source"
  val CsvCurioTeacherSource = "csv-curio-teacher-source"
  val ParquetCurioStudentSink = "parquet-curio-student-sink"
  val ParquetCurioTeacherSink = "parquet-curio-teacher-sink"

  val ParquetGuardianXApiV2Source = "parquet-guardian-xapi-v2-source"
  val ParquetStudentXApiV2Source = "parquet-student-xapi-v2-source"
  val ParquetTeacherXApiV2Source = "parquet-teacher-xapi-v2-source"
  val ParquetInvalidTeacherXApiV2Sink = "parquet-invalid-teacher-xapi-v2-sink"

  val ParquetAcademicYearStartedSource = "parquet-academic-year-started-source"
  val ParquetAcademicYearUpdatedSource = "parquet-academic-year-date-updated-source"
  val ParquetAcademicYearRollOverCompletedSource = "parquet-academic-year-roll-over-completed-source"
  val ParquetAcademicYearStartedSink = "parquet-academic-year-started-sink"
  val ParquetAcademicYearUpdatedSink = "parquet-academic-year-date-updated-sink"
  val ParquetAcademicYearRollOverCompletedSink = "parquet-academic-year-roll-over-completed-sink"


  val RedshiftSource = "redshift"

  val RedshiftStudentSink = "redshift-student-rel"
  val RedshiftDimStudentSink = "redshift-student-dim"
  val RedshiftTeacherSink = "redshift-teacher-rel"

  val RedshiftGuardianRelSink = "redshift-guardian-rel"
  val RedshiftGuardianDimSource = "redshift-guardian-dim"
  val DeltaGuardianSink = "delta-guardian-dim"

  val RedshiftAdminUserSink = "redshift-admin-rel"
  val RedshiftAdminUserDimSink = "redshift-admin-dim"
  val DeltaAdminUserSink = "delta-admin-sink"
  val RedshiftUserSink = "redshift-user-rel"

  val RedshiftStudentGuardianSink = "redshift-guardian-dim"
  val RedshiftDimTeacherSink = "redshift-teacher-dim"
  val RedshiftSkillSink = "redshift-skill-dim"

  val RedshiftCurioStudentSink = "redshift-curio-student-sink"
  val RedshiftCurioTeacherSink = "redshift-curio-teacher-sink"

  val RedshiftCurriculumCreatedSink = "redshift-curriculum-created-sink"
  val RedshiftCurriculumGradeCreatedSink = "redshift-curriculum-grade-created-sink"
  val RedshiftCurriculumSubjectSink = "redshift-curriculum-subject-sink"

  val DeltaCurriculumSubjectCreatedSink = "delta-curriculum-subject-created-sink"
  val DeltaCurriculumSubjectMutatedSink = "delta-curriculum-subject-sink"

  val ParquetContentOutcomeSink = "parquet-content-outcome-source"
  val ParquetContentSkillSink = "parquet-content-skill-source"
  val ParquetContentClassSink = ParquetContentClassSource
  val RedshiftContentAssociationSink = "redshift-content-association-sink"
  val DeltaContentAssociationSink = "delta-content-association-sink"

  val RedshiftStudentContentAssignmentSink = "redshift-content-student-assignment-sink"
  val RedshiftStudentContentUnassignmentSink = "redshift-content-student-unassignment-sink"
  val RedshiftClassContentAssignmentSink = "redshift-content-class-assignment-sink"
  val RedshiftClassContentUnAssignmentSink = "redshift-content-class-unassignment-sink"
  val DeltaContentStudentAssignmentSink = "delta-content-student-assignment-sink"
  val DeltaContentClassAssignmentSink = "delta-content-class-assignment-sink"

  val RedshiftStudentLessonAssignmentSink = "redshift-lesson-student-assignment-sink"
  val RedshiftStudentLessonUnassignmentSink = "redshift-lesson-student-unassignment-sink"
  val RedshiftClassLessonAssignmentSink = "redshift-lesson-class-assignment-sink"
  val RedshiftClassLessonUnAssignmentSink = "redshift-lesson-class-unassignment-sink"
  val DeltaLessonStudentAssignmentSink = "delta-lesson-student-assignment-sink"
  val DeltaLessonClassAssignmentSink = "delta-lesson-class-assignment-sink"

  val ParquetContentCreatedSink = "parquet-content-created-source"
  val ParquetContentUpdatedSink = "parquet-content-updated-source"
  val ParquetContentPublishedSink = "parquet-content-published-source"
  val ParquetContentDeletedSink = "parquet-content-deleted-source"
  val RedshiftContentCreatedSink = "redshift-content-created-source"
  val RedshiftContentUpdatedSink = "redshift-content-updated-source"
  val RedshiftContentPublishedSink = "redshift-content-published-source"
  val RedshiftContentDeletedSink = "redshift-content-deleted-source"

  val ParquetOutcomeCreatedSource = "parquet-outcome-created-source"
  val ParquetOutcomeUpdatedSource = "parquet-outcome-updated-source"
  val ParquetOutcomeDeletedSource = "parquet-outcome-deleted-source"

  val RedshiftOutcomeSink = "redshift-outcome-sink"

  val DeltaOutcomeSink = "delta-outcome-sink"

  val DeltaStudentSink = "delta-student-sink"

  val DeltaContentCreatedSink = "delta-content-created-sink"
  val DeltaContentUpdatedSink = "delta-content-updated-sink"
  val DeltaContentPublishedSink = "delta-content-published-sink"
  val DeltaContentDeletedSink = "delta-content-deleted-sink"

  val DeltaContentSink = "delta-content-sink"

  val RedshiftSchoolSource, RedshiftSchoolSink = "redshift-school-dim"
  val RedshiftOrganizationSource = "redshift-organization-dim"
  val RedshiftDwIdMappingSource = "redshift-dw-id-mapping-source"
  val RedshiftContentRepositorySource = "redshift-content-repository-source"

  val DeltaSchoolSink = "delta-school-sink"

  val RedshiftTenantSource = "redshift-tenant-dim"
  val RedshiftRoleSource = "redshift-role-dim"
  val RedshiftCxRoleSource = "redshift-role-cx-dim-sink"
  val RedshiftSubjectSource, RedshiftSubjectSink = "redshift-subject-dim"
  val RedshiftSectionSource, RedshiftSectionSink = "redshift-section-dim"
  val RedshiftGradeSource, RedshiftGradeSink = "redshift-grade-dim"
  val RedshiftLearningPathSource, RedshiftLearningPathSink = "redshift-learning-path-dim"
  val RedshiftStarAwardSource = "redshift-star-award-source"
  val RedshiftStarAwardSink = "redshift-star-award-sink"
  val DeltaStarAwardSink = "delta-star-award-sink"
  val ConversationOccurredService = "conversation-occurred"
  val RedshiftConversationOccurredSink = "redshift-conversation-occurred-sink"
  val DeltaConversationOccurredSink = "delta-conversation-occurred-sink"
  val RedshiftPracticeSink = "redshift-practice-sink"
  val DeltaPracticeSink = "delta-practice-sink"
  val RedshiftNoSkillContentSink = "redshift-no-skill-content-sink"
  val PracticeService = "practice"
  val RedshiftAcademicYearSink = "redshift-academic-year-dim"

  val GuardianXapiV2Service = "guardian-xapi-v2-service"
  val StudentXapiV2Service = "student-xapi-v2-service"
  val TeacherXapiV2Service = "teacher-xapi-v2-service"

  val UserLoginAggregationService = "user-login-aggregation"
  val UserLoginAggregationTransformService = "user-login-transform-aggregation"
  val UserLoginAggregationParquetService = "user-login-parquet-aggregation"
  val UserLoginAggregationRedshiftService = "user-login-redshift-aggregation"
  val UserLoginAggregationDeltaService = "user-login-delta-aggregation"

  val ParquetUserLoginSource = "parquet-authentication-loggedIn-source"
  val ParquetUserLoginSink = "parquet-authentication-loggedIn-sink"
  val ParquetUserLoginTransformedSource = "parquet-authentication-transform-source"
  val ParquetUserLoginTransformedSink = "parquet-authentication-transform-sink"
  val RedshiftUserLoginSink = "redshift-user-login-sink"
  val UserLoginDeltaSink = "delta-user-login-sink"

  val SourceAwardResourceAgg = "source-award-resource-aggregation"

  val RedshiftLearningExperienceSink = "redshift-learning-experience-sink"
  val ParquetTotalScoreUpdatedSource, ParquetTotalScoreUpdatedSink = "parquet-total-score-updated-source"

  val RedshiftPracticeSessionSink = "redshift-practice-session-sink"
  val DeltaPracticeSessionSink = "delta-practice-session-sink"

  val DeltaIncGameSink = "delta-in-class-game-sink"
  val DeltaIncGameOutcomeSink = "delta-in-class-game-outcome-sink"
  val DeltaIncGameSessionSink = "delta-in-class-game-session-sink"

  val RedshiftIncGameSink = "redshift-in-class-game-sink"
  val RedshiftIncGameSessionSink = "redshift-in-class-game-session-sink"
  val RedshiftIncGameOutcomeSink = "redshift-in-class-game-outcome-sink"

  val RedshiftTeacherActivitiesSink = "redshift-teacher-activities-sink"
  val RedshiftStudentActivitiesSink = "redshift-student-activities-sink"
  val RedshiftGuardianAppActivitiesSink = "redshift-guardian-app-activities-sink"

  val DeltaGuardianAppActivitiesSink = "delta-guardian-app-activities-sink"

  val RedshiftLearningObjectiveSink = "redshift-learning-objective-dim"
  val ParquetLearningObjectiveCreatedSource = "parquet-learning-objective-source"
  val ParquetLearningObjectiveMutatedSource = "parquet-learning-objective-updated-deleted-source"
  val ParquetCCLLessonMutatedSource = "parquet-ccl-lesson-mutated-source"
  val ParquetCCLLessonDeletedSource = "parquet-ccl-lesson-deleted-source"
  val ParquetCCLLessonPublishedSource = "parquet-ccl-lesson-published-source"
  val ParquetCCLLessonWorkflowStatusChangedSource = "parquet-ccl-lesson-workflow-status-changed-source"
  val ParquetCCLLessonOutcomeAttachSource = "parquet-ccl-lesson-outcome-attach-source"
  val ParquetCCLLessonContentAttachSource = "parquet-ccl-lesson-content-attach-source"
  val ParquetCCLLessonSkillLinkSource, ParquetCCLLessonSkillLinkSink = "parquet-ccl-lesson-skill-link-source"
  val ParquetCCLLessonAssigmentAttachSource = "parquet-ccl-lesson-assigment-attach-source"
  val ParquetCCLLessonAssessmentRuleSource = "parquet-ccl-lesson-assessment-rule-source"
  val RedshiftLessonStepInstanceSink = "redshift-ccl-lesson-step-instance-dim"
  val RedshiftLearningObjectiveAssociationDimSink = "redshift-learning-objective-association-dim"

  val ParquetCCLActivityTemplateSource, ParquetCCLActivityTemplateSink = "parquet-ccl-activity-template-source"
  val RedshiftCCLActivityTemplateSink = "redshift-ccl-activity-template-sink"
  val DeltaCCLActivityTemplateSink = "delta-ccl-activity-template-sink"

  val ParquetCCLActivityMetadataSource, ParquetCCLActivityMetadataSink = "parquet-ccl-activity-metadata-source"
  val ParquetCCLActivityMetadataDeleteSource, ParquetCCLActivityMetadataDeleteSink = "parquet-ccl-activity-metadata-delete-source"
  val ParquetCCLActivityWorkflowStatusChangedSource, ParquetCCLActivityWorkflowStatusChangedSink = "parquet-ccl-activity-workflow-status-changed-source"
  val DeltaCCLActivitySink = "delta-ccl-activity-sink"

  val ParquetCCLActivityOutcomeSource, ParquetCCLActivityOutcomeSink = "parquet-ccl-activity-outcome-source"

  val DeltaCCLLessonCreatedSink = "delta-ccl-lesson-created-sink"
  val DeltaCCLLessonUpdatedSink = "delta-ccl-lesson-updated-sink"
  val DeltaCCLLessonPublishedSink = "delta-ccl-lesson-published-sink"
  val DeltaCCLLessonDeletedSink = "delta-ccl-lesson-deleted-sink"
  val DeltaCCLLessonWorkflowStatusChangedSink = "delta-ccl-lesson-workflow-status-changed-sink"
  val ParquetCCLSkillMutatedSource = "parquet-ccl-skill-mutated"
  val ParquetCCLSkillDeletedSource = "parquet-ccl-skill-deleted"
  val RedshiftCCLSkillDimSink = "redshift-ccl-skill-sink"
  val DeltaCCLSkillSink = "delta-ccl-skill-sink"
  val ParquetSkillLinkToggleSource = "parquet-skill-association"
  val ParquetSkillLinkToggleV2Source = "parquet-skill-association-v2"
  val ParquetSkillCategoryLinkToggleSource = "parquet-skill-category-association"
  val RedshiftSkillAssociationSink = "redshift-skill-association-sink"
  val DeltaSkillAssociationSink = "delta-skill-association-sink"

  val AdminUsersDimensionName = "admin-users-dimension"
  val ContentDimensionName = "content-dimension"
  val CurriculumDimensionName = "curriculum-dimension"
  val ContentAssociationDimensionName = "content-association-dimension"
  val SkillAssociationDimensionName = "skill-association-dimension"
  val ContentStudentAssignmentDimensionName = "content-student-assignment-dimension"
  val LessonAssignmentDimensionName = "lesson-assignment-dimension"
  val SectionDimensionName = "section-dimension"
  val GradeDimensionName = "grade-dimension"
  val GuardianDimensionName = "guardian-dimension"
  val LearningObjectiveDimensionName = "learning-objective-dimension"
  val CCLLessonDimensionName = "ccl-lesson-dimension"
  val CCLLessonAssociationDimensionName = "ccl-lesson-association-dimension"
  val CCLLessonStepInstanceAssociationDimensionName = "ccl-lesson-step-instance-association"
  val CCLActivityTemplateDimensionName = "ccl-activity-template-dimension"
  val CCLActivityDimensionName = "ccl-activity-dimension"
  val CCLActivityAssociationDimensionName = "ccl-activity-association-dimension"
  val LearningPathDimensionName = "learning-path-dimension"
  val StudentDimensionName = "student-dimension"
  val SubjectDimensionName = "subject-dimension"
  val AcademicYearDimensionName = "academic-year-dimension"
  val AcademicYearDeltaSink = "delta-academic-year-sink"
  val OutcomeDimensionName = "outcome-dimension"
  val CourseDimensionName = "course-dimension"
  val ClassDimensionName = "class-dimension"
  val OutcomeAssociationDimensionName = "outcome-association-dimension"
  val TagDimensionName = "tag-dimension"

  val DeltaCCLLessonContentStepInstanceSink = "delta-ccl-lesson-step-instance-sink"
  val DeltaCCLLessonOutcomeAttachSink = "delta-ccl-lesson-outcome-attach-sink"
  val DeltaCCLLessonSkillLinkSink = "delta-ccl-lesson-skill-link-sink"

  val DeltaCCLActivityAssociationSink = "delta-ccl-activity-association-sink"

  val AssignmentInstanceDimensionName = "assignment-instance-dimension"
  val InstructionalPlanDimensionName = "instructional-plan-dimension"
  val AssignmentDimensionName = "assignment-dimension"

  val RedshiftInstructionalPlanSink = "redshift-ip-sink"
  val ParquetInstructionalPlanPublishedSource = "parquet-ip-published-source"
  val ParquetInstructionalPlanRePublishedSource = "parquet-ip-re-published-source"
  val ParquetInstructionalPlanDeletedSource = "parquet-ip-deleted-source"
  val InstructionalPlanDeltaSink = "delta-ip-sink"

  val ParquetClassModifiedSource = "parquet-class-modified-source"
  val ParquetClassDeletedSource = "parquet-class-deleted-source"
  val ParquetStudentClassAssociationSource = "parquet-student-class-association-source"
  val DeltaClassSink = "delta-class-sink"
  val RedshiftClassUserSink = "redshift-class-user-sink"
  val DeltaClassUserSink = "delta-class-user-sink"
  val RedshiftClassStageSink = "redshift-class-stage-sink"
  val RedshiftDwIdMappingsStageSink = "redshift-dw-id-mappings-stage-sink"

  val PracticeFactName = "practice-fact"

  val incGameName = "in-class-game"

  val termDimensionName = "term-dimension"
  val termEntity = "term"
  val weekEntity = "week"
  val parquetTermWeekSource = "parquet-term-week-source"
  val deltaTermSink = "delta-term-sink"
  val redshiftTermSink = "redshift-term-sink"
  val redshiftWeekSink = "redshift-week-sink"
  val deltaWeekSink = "delta-week-sink"
  val termWeekCreatedEvent = "TermCreatedEvent"

  val ContentEntity = "content"
  val ContentAssociationEntity = "content_association"
  val SkillAssociationEntity = "skill_association"
  val ContentStudentAssociationEntity = "content_student_association"
  val LessonAssignmentEntity = "lesson_assignment"
  val CurriculumEntity = "curr"
  val CurriculumGradeEntity = "curr_grade"
  val CurriculumSubjectEntity = "curr_subject"
  val SectionEntity = "section"
  val GradeEntity = "grade"
  val LearningObjectiveEntity = "lo"
  val LearningObjectiveAssociationEntity = "lo_association"
  val LearningObjectiveStepInstanceEntity = "step_instance"
  val ActivityTemplateEntity = "at"
  val SchoolEntity = "school"
  val SubjectEntity = "subject"
  val TeacherEntity = "teacher"
  val StudentEntity = "student"
  val GuardianEntity = "guardian"
  val GuardianInvitationEntity = "guardian_invitation"
  val AdminUserEntity = "admin"
  val FactLearningExperienceEntity = "fle"
  val FactExperienceSubmittedEntity = "fes"
  val FactTeacherActivities = "fta"
  val FactStudentActivities = "fsta"
  val FactGuardianAppActivities = "fgaa"
  val FactPracticeSessionEntity = "practice_session"
  val FactKtGameSessionEntity = "ktg_session"
  val FactIncGameEntity = "inc_game"
  val FactIncGameSessionEntity = "inc_game_session"
  val FactIncGameOutcomeEntity = "inc_game_outcome"
  val FactStarAwardedEntity = "fsa"
  val FactUserLoginEntity = "ful"
  val FactWeeklyGoalEntity = "fwg"
  val FactWeeklyGoalActivityEntity = "fwga"
  val FactConversationOccurredEntity = "fco"
  val FactPractice = "practice"
  val LearningPathEntity = "learning_path"
  val SkillEntity = "skill"
  val CCLSkillEntity = "ccl_skill"
  val AcademicYearEntity = "academic_year"
  val ClassEntity = "class"
  val ClassUserEntity = "class_user"
  val OutcomeEntity = "outcome"
  val OutcomeAssociationEntity = "outcome_association"
  val TagEntity = "tag"
  val ClassScheduleEntity = "class_schedule"

  val SectionScheduleEntity = "section_schedule"
  val LessonFeedbackEntity: String = "lesson_feedback"
  val NoSkillContentEntity: String = "scu"
  val AssignmentInstanceEntity = "assignment_instance"
  val AssignmentInstanceStudentEntity = "ais"
  val InstructionPlanEntity = "instructional_plan"
  val AssignmentSubmissionEntity = "assignment_submission"
  val AssignmentEntity = "assignment"
  val PathwayLevelActivityAssociationEntity = "plaa"
  val PathwayLevelActivityAssociationLearningOutcomeEntity = "plaa_outcome"

  val AssignmentSubmissionName = "assignment-submission"
  val AssignmentSubmissionSource = "parquet-assignment-submission-source"
  val AssignmentSubmissionDeltaSink = "delta-assignment-submission-sink"
  val AssignmentSubmissionRedshiftSink = "redshift-assignment-submission-sink"

  val RedshiftOutcomeAssociationSink = "redshift-outcome-association-sink"
  val DeltaOutcomeAssociationSink = "delta-outcome-association-sink"

  val RedshiftTagSink = "redshift-tag-sink"
  val DeltaTagSink = "delta-tag-sink"

  val ClassScheduleDimensionName = "class-schedule-dimension"

  val ParquetClassScheduleSource, ParquetClassScheduleSink = "parquet-class-schedule"
  val RedshiftClassScheduleSink = "redshift-class-schedule-sink"
  val DeltaClassScheduleSink = "delta-class-schedule-sink"

  val ExperienceSubmittedName = "experience-submitted"

  val ParquetExperienceSubmittedSource, ParquetExperienceSubmittedSink = "parquet-experience-submitted-source"
  val RedshiftExperienceSubmittedSink = "redshift-experience-submitted-sink"
  val DeltaExperienceSubmittedSink = "delta-experience-submitted-sink"

  val LearningSessionCols = Map(
    "experienceId" -> "fle_exp_id",
    "learningSessionId" -> "fle_ls_id",
    "contentId" -> "fle_step_id",
    "learningObjectiveId" -> "lo_uuid",
    "eventDateDw" -> "fle_date_dw_id",
    "studentId" -> "student_uuid",
    "subjectId" -> "subject_uuid",
    "studentGradeId" -> "grade_uuid",
    "tenantId" -> "tenant_uuid",
    "schoolId" -> "school_uuid",
    "studentSection" -> "section_uuid",
    "classId" -> "class_uuid",
    "startTime" -> "fle_start_time",
    "endTime" -> "fle_end_time",
    "totalTime" -> "fle_total_time",
    "score" -> "fle_score",
    "stars" -> "fle_star_earned",
    "lessonType" -> "fle_lesson_type",
    "retry" -> "fle_is_retry",
    "outsideOfSchool" -> "fle_outside_of_school",
    "attempt" -> "fle_attempt",
    "learningExperienceFlag" -> "fle_exp_ls_flag",
    "learningPathId" -> "lp_uuid",
    "instructionalPlanId" -> "fle_instructional_plan_id",
    "trimesterOrder" -> "fle_academic_period_order",
    "curriculumId" -> "curr_uuid",
    "curriculumGradeId" -> "curr_grade_uuid",
    "curriculumSubjectId" -> "curr_subject_uuid",
    "occurredOn" -> "occurredOn",
    "academicYearId" -> "academic_year_uuid",
    "contentAcademicYear" -> "fle_content_academic_year",
    "timeSpent" -> "fle_time_spent_app",
    "lessonCategory" -> "fle_lesson_category",
    "materialId" -> "fle_material_id",
    "materialType" -> "fle_material_type",
    "openPathEnabled" -> "fle_open_path_enabled",
    "totalScore" -> "fle_total_score",
    "activityTemplateId" -> "fle_activity_template_id",
    "activityType" -> "fle_activity_type",
    "activityComponentType" -> "fle_activity_component_type",
    "abbreviation" -> "fle_abbreviation",
    "exitTicket" -> "fle_exit_ticket",
    "mainComponent" -> "fle_main_component",
    "completionNode" -> "fle_completion_node",
    "activityCompleted" -> "fle_is_activity_completed",
    "state" -> "fle_state",
    "totalStars" -> "fle_total_stars",
    "eventType" -> "eventType",
    "activityComponentResources" -> "fle_activity_component_resource",
    "source" -> "fle_source",
    "teachingPeriodId" -> "fle_teaching_period_id",
    "academicYear" -> "fle_academic_year",
    "isAdditionalResource" -> "fle_is_additional_resource",
    "bonusStars" -> "fle_bonus_stars",
    "bonusStarsScheme" -> "fle_bonus_stars_scheme"
  )

  val LearningExperienceCols = LearningSessionCols + (
    "level" -> "fle_adt_level",
    "scoreBreakDown" -> "fle_score_breakdown",
    "activityComponentResources" -> "fle_activity_component_resource",
    "suid" -> "fle_assessment_id",
    "isInClassGameExperience" -> "fle_is_gamified"
  )

  val ExperienceSubmittedCols = Map(
    "uuid" -> "fes_id",
    "startTime" -> "fes_start_time",
    "experienceId" -> "exp_uuid",
    "learningSessionId" -> "fes_ls_id",
    "learningObjectiveId" -> "lo_uuid",
    "contentId" -> "fes_step_id",
    "studentId" -> "student_uuid",
    "studentSection" -> "section_uuid",
    "subjectId" -> "subject_uuid",
    "classId" -> "class_uuid",
    "studentGradeId" -> "grade_uuid",
    "tenantId" -> "tenant_uuid",
    "schoolId" -> "school_uuid",
    "learningPathId" -> "lp_uuid",
    "instructionalPlanId" -> "fes_instructional_plan_id",
    "contentPackageId" -> "fes_content_package_id",
    "contentTitle" -> "fes_content_title",
    "contentType" -> "fes_content_type",
    "lessonType" -> "fes_lesson_type",
    "redo" -> "fes_is_retry",
    "outsideOfSchool" -> "fes_outside_of_school",
    "attempt" -> "fes_attempt",
    "trimesterOrder" -> "fes_academic_period_order",
    "academicYearId" -> "academic_year_uuid",
    "contentAcademicYear" -> "fes_content_academic_year",
    "lessonCategory" -> "fes_lesson_category",
    "occurredOn" -> "occurredOn",
    "suid" -> "fes_suid",
    "abbreviation" -> "fes_abbreviation",
    "activityType" -> "fes_activity_type",
    "exitTicket" -> "fes_exit_ticket",
    "mainComponent" -> "fes_main_component",
    "completionNode" -> "fes_completion_node",
    "activityComponentType" -> "fes_activity_component_type",
    "materialId" -> "fes_material_id",
    "materialType" -> "fes_material_type",
    "openPathEnabled" -> "fes_open_path_enabled",
    "teachingPeriodId" -> "fes_teaching_period_id",
    "academicYear" -> "fes_academic_year"
  )

  val ExperienceTotalScoreUpdatedCols = Map(
    "uuid" -> "fle_exp_id",
    "learningSessionId" -> "fle_ls_id",
    "learningObjectiveId" -> "lo_uuid",
    "eventDateDw" -> "fle_date_dw_id",
    "studentId" -> "student_uuid",
    "studentGradeId" -> "grade_uuid",
    "studentSection" -> "section_uuid",
    "instructionalPlanId" -> "fle_instructional_plan_id",
    "subjectId" -> "subject_uuid",
    "eventDateDw" -> "fle_date_dw_id",
    "tenantId" -> "tenant_uuid",
    "schoolId" -> "school_uuid",
    "learningPathId" -> "lp_uuid",
    "curriculumId" -> "curr_uuid",
    "curriculumGradeId" -> "curr_grade_uuid",
    "curriculumSubjectId" -> "curr_subject_uuid",
    "outsideOfSchool" -> "fle_outside_of_school",
    "attempt" -> "fle_attempt",
    "trimesterOrder" -> "fle_academic_period_order",
    "retry" -> "fle_is_retry",
    "classId" -> "class_uuid",
    "startTime" -> "fle_start_time",
    "endTime" -> "fle_end_time",
    "totalTime" -> "fle_total_time",
    "academicYearId" -> "academic_year_uuid",
    "contentAcademicYear" -> "fle_content_academic_year",
    "lessonCategory" -> "fle_lesson_category",
    "score" -> "fle_score",
    "stars" -> "fle_star_earned",
    "totalScore" -> "fle_total_score",
    "totalStars" -> "fle_total_stars",
    "timeSpent" -> "fle_time_spent_app",
    "activityCompleted" -> "fle_is_activity_completed",
    "activityType" -> "fle_activity_type",
    "activityTemplateId" -> "fle_activity_template_id",
    "lessonType" -> "fle_lesson_type",
    "activityType" -> "fle_activity_type",
    "activityComponentType" -> "fle_activity_component_type",
    "abbreviation" -> "fle_abbreviation",
    "exitTicket" -> "fle_exit_ticket",
    "mainComponent" -> "fle_main_component",
    "completionNode" -> "fle_completion_node",
    "occurredOn" -> "occurredOn",
    "materialId" -> "fle_material_id",
    "materialType" -> "fle_material_type",
    "state" -> "fle_state",
    "openPathEnabled"-> "fle_open_path_enabled",
    "activityComponents" -> "fle_activity_components",
    "teachingPeriodId" -> "fle_teaching_period_id",
    "academicYear" -> "fle_academic_year",
  )

  lazy val StagingStarAwarded = Map(
    "id" -> "fsa_id",
    "categoryCode" -> "award_category_uuid",
    "learnerId" -> "student_uuid",
    "tenantId" -> "tenant_uuid",
    "schoolId" -> "school_uuid",
    "classId" -> "class_uuid",
    "subjectId" -> "subject_uuid",
    "teacherId" -> "teacher_uuid",
    "gradeId" -> "grade_uuid",
    "comment" -> "fsa_award_comments",
    "eventDateDw" -> "fsa_date_dw_id",
    "occurredOn" -> "occurredOn",
    "academicYearId" -> "academic_year_uuid",
    "stars" -> "fsa_stars"
  )

  lazy val StagingUserLogin = Map(
    "id" -> "ful_id",
    "tenantId" -> "tenant_uuid",
    "schoolId" -> "school_uuid",
    "outsideOfSchool" -> "ful_outside_of_school",
    "role" -> "role_uuid",
    "eventDateDw" -> "ful_date_dw_id",
    "uuid" -> "user_uuid",
    "loginTime" -> "ful_login_time",
    "occurredOn" -> "occurredOn"
  )

  lazy val StagingConversationOccurred = Map(
    "tenantId" -> "tenant_uuid",
    "conversationId" -> "fco_id",
    "userId" -> "student_uuid",
    "schoolId" -> "school_uuid",
    "gradeId" -> "grade_uuid",
    "sectionId" -> "section_uuid",
    "mloId" -> "lo_uuid",
    "question" -> "fco_question",
    "answerId" -> "fco_answer_id",
    "answer" -> "fco_answer",
    "arabicAnswer" -> "fco_arabic_answer",
    "source" -> "fco_source",
    "suggestions" -> "fco_suggestions",
    "eventDateDw" -> "fco_date_dw_id",
    "occurredOn" -> "occurredOn",
    "learningSessionId" -> "fco_learning_session_id",
    "subject" -> "fco_subject_category"
  )

  lazy val StagingPractice = Map(
    "tenantId" -> "tenant_uuid",
    "id" -> "practice_id",
    "learnerId" -> "student_uuid",
    "schoolId" -> "school_uuid",
    "gradeId" -> "grade_uuid",
    "subjectId" -> "subject_uuid",
    "sectionId" -> "section_uuid",
    "learningObjectiveId" -> "lo_uuid",
    "skillId" -> "skill_uuid",
    "saScore" -> "practice_sa_score",
    "practiceItemLearningObjectiveId" -> "item_lo_uuid",
    "practiceItemSkillId" -> "item_skill_uuid",
    "practiceItemContentId" -> "practice_item_step_id",
    "practiceItemContentTitle" -> "practice_item_content_title",
    "practiceItemContentLessonType" -> "practice_item_content_lesson_type",
    "practiceItemContentLocation" -> "practice_item_content_location",
    "eventDateDw" -> "practice_date_dw_id",
    "occurredOn" -> "occurredOn",
    "academicYearId" -> "academic_year_uuid",
    "instructionalPlanId" -> "practice_instructional_plan_id",
    "learningPathId" -> "practice_learning_path_id",
    "classId" -> "class_uuid",
    "materialId" -> "practice_material_id",
    "materialType" -> "practice_material_type"
  )

  lazy val PracticeDeltaColumns = Map(
    "tenant_uuid" -> "practice_tenant_id",
    "practice_id" -> "practice_id",
    "student_uuid" -> "practice_student_id",
    "school_uuid" -> "practice_school_id",
    "grade_uuid" -> "practice_grade_id",
    "subject_uuid" -> "practice_subject_id",
    "section_uuid" -> "practice_section_id",
    "lo_uuid" -> "practice_lo_id",
    "skill_uuid" -> "practice_skill_id",
    "practice_sa_score" -> "practice_sa_score",
    "item_lo_uuid" -> "practice_item_lo_id",
    "item_skill_uuid" -> "practice_item_skill_id",
    "practice_item_step_id" -> "practice_item_step_id",
    "practice_item_content_title" -> "practice_item_content_title",
    "practice_item_content_lesson_type" -> "practice_item_content_lesson_type",
    "practice_item_content_location" -> "practice_item_content_location",
    "academic_year_uuid" -> "practice_academic_year_id",
    "practice_instructional_plan_id" -> "practice_instructional_plan_id",
    "practice_learning_path_id" -> "practice_learning_path_id",
    "class_uuid" -> "practice_class_id",
    "practice_created_time" -> "practice_created_time",
    "practice_dw_created_time" -> "practice_dw_created_time",
    "practice_date_dw_id" -> "practice_date_dw_id",
    "practice_material_id" -> "practice_material_id",
    "practice_material_type" -> "practice_material_type"
  )

  val CommonPracticeColsMapping = Map(
    "practiceId" -> "practice_session_id",
    "tenantId" -> "tenant_uuid",
    "occurredOn" -> "occurredOn",
    "eventDateDw" -> "practice_session_date_dw_id",
    "type" -> "practice_session_event_type",
    "isStart" -> "practice_session_is_start",
    "isStartProcessed" -> "practice_session_is_start_event_processed",
    "outsideOfSchool" -> "practice_session_outside_of_school",
    "stars" -> "practice_session_stars",
    "instructionalPlanId" -> "practice_session_instructional_plan_id",
    "learningPathId" -> "practice_session_learning_path_id",
    "materialId" -> "practice_session_material_id",
    "materialType" -> "practice_session_material_type"
  )

  lazy val StagingNoSkillContent = Map(
    "tenantId" -> "tenant_uuid",
    "eventDateDw" -> "scu_date_dw_id",
    "learningObjectiveId" -> "lo_uuid",
    "occurredOn" -> "occurredOn",
    "skillId" -> "skill_uuid"
  )

  val StagingPracticeSession = CommonPracticeColsMapping ++ Map(
    "learnerId" -> "student_uuid",
    "subjectId" -> "subject_uuid",
    "schoolId" -> "school_uuid",
    "gradeId" -> "grade_uuid",
    "sectionId" -> "section_uuid",
    "learningObjectiveId" -> "lo_uuid",
    "saScore" -> "practice_session_sa_score",
    "score" -> "practice_session_score",
    "academicYearId" -> "academic_year_uuid",
    "classId" -> "class_uuid"
  )

  val StagingPracticeItemSession = CommonPracticeColsMapping ++ Map(
    "learningObjectiveId" -> "practice_session_item_lo_uuid",
    "score" -> "practice_session_score",
    "practiceLearnerId" -> "student_uuid",
    "practiceSubjectId" -> "subject_uuid",
    "practiceSchoolId" -> "school_uuid",
    "practiceGradeId" -> "grade_uuid",
    "practiceSectionId" -> "section_uuid",
    "practiceLearningObjectiveId" -> "lo_uuid",
    "practiceSaScore" -> "practice_session_sa_score",
    "practiceAcademicYearId" -> "academic_year_uuid",
    "practiceClassId" -> "class_uuid"
  )

  val StagingPracticeItemContentSession = CommonPracticeColsMapping ++ Map(
    "id" -> "practice_session_item_step_id",
    "title" -> "practice_session_item_content_title",
    "lessonType" -> "practice_session_item_content_lesson_type",
    "location" -> "practice_session_item_content_location",
    "score" -> "practice_session_score",
    "practiceLearnerId" -> "student_uuid",
    "practiceSubjectId" -> "subject_uuid",
    "practiceSchoolId" -> "school_uuid",
    "practiceGradeId" -> "grade_uuid",
    "practiceSectionId" -> "section_uuid",
    "practiceLearningObjectiveId" -> "lo_uuid",
    "practiceSaScore" -> "practice_session_sa_score",
    "practiceItemLearningObjectiveId" -> "practice_session_item_lo_uuid",
    "practiceAcademicYearId" -> "academic_year_uuid",
    "practiceClassId" -> "class_uuid"
  )

  lazy val teacherXAPIColumnMapping = Map(
    "tenantId" -> "tenant_uuid",
    "actorAccountName" -> "teacher_uuid",
    "extensions.role" -> "role_uuid",
    "actorObjectType" -> "fta_actor_object_type",
    "actorAccountHomePage" -> "fta_actor_account_homepage",
    "verbId" -> "fta_verb_id",
    "verbDisplay" -> "fta_verb_display",
    "objectId" -> "fta_object_id",
    "objectType" -> "fta_object_type",
    "objectDefinitionType" -> "fta_object_definition_type",
    "objectDefinitionName" -> "fta_object_definition_name",
    "categoryId" -> "fta_context_category",
    "schoolId" -> "school_uuid",
    "gradeId" -> "grade_uuid",
    "sectionId" -> "section_uuid",
    "subjectId" -> "subject_uuid",
    "extensions.outsideOfSchool" -> "fta_outside_of_school",
    "eventType" -> "fta_event_type",
    "prevEventType" -> "fta_prev_event_type",
    "nextEventType" -> "fta_next_event_type",
    "timestamp" -> "fta_start_time",
    "endTime" -> "fta_end_time",
    "timeSpent" -> "fta_time_spent",
    "timestampLocal" -> "fta_timestamp_local",
    "eventDateDw" -> "fta_date_dw_id",
    "occurredOn" -> "occurredOn"
  )

  lazy val studentXAPIColumnMapping = Map(
    "tenantId" -> "tenant_uuid",
    "actorAccountName" -> "student_uuid",
    "actorObjectType" -> "fsta_actor_object_type",
    "actorAccountHomePage" -> "fsta_actor_account_homepage",
    "verbId" -> "fsta_verb_id",
    "verbDisplay" -> "fsta_verb_display",
    "objectId" -> "fsta_object_id",
    "objectType" -> "fsta_object_type",
    "objectDefinitionType" -> "fsta_object_definition_type",
    "objectDefinitionName" -> "fsta_object_definition_name",
    "schoolId" -> "school_uuid",
    "gradeId" -> "grade_uuid",
    "sectionId" -> "section_uuid",
    "subjectId" -> "subject_uuid",
    "experienceId" -> "fsta_exp_id",
    "learningSessionId" -> "fsta_ls_id",
    "academicYearId" -> "academic_year_uuid",
    "extensions.outsideOfSchool" -> "fsta_outside_of_school",
    "isCompletionNode" -> "fsta_is_completion_node",
    "isFlexibleLesson" -> "fsta_is_flexible_lesson",
    "academicCalendarId" -> "fsta_academic_calendar_id",
    "teachingPeriodId" -> "fsta_teaching_period_id",
    "teachingPeriodTitle" -> "fsta_teaching_period_title",
    "eventType" -> "fsta_event_type",
    "prevEventType" -> "fsta_prev_event_type",
    "nextEventType" -> "fsta_next_event_type",
    "timestamp" -> "fsta_start_time",
    "endTime" -> "fsta_end_time",
    "timeSpent" -> "fsta_time_spent",
    "timestampLocal" -> "fsta_timestamp_local",
    "eventDateDw" -> "fsta_date_dw_id",
    "rawScore" -> "fsta_score_raw",
    "scaledScore" -> "fsta_score_scaled",
    "minScore" -> "fsta_score_min",
    "maxScore" -> "fsta_score_max",
    "fromTime" -> "fsta_from_time",
    "toTime" -> "fsta_to_time",
    "attempt" -> "fsta_attempt",
    "lessonPosition" -> "fsta_lesson_position",
    "occurredOn" -> "occurredOn"
  )

  lazy val guardianXAPIColumnMapping = Map(
    "tenantId" -> "tenant_uuid",
    "actorAccountName" -> "guardian_uuid",
    "actorAccountHomePage" -> "fgaa_actor_account_homepage",
    "objectAccountHomePage" -> "fgaa_object_account_homepage",
    "actorObjectType" -> "fgaa_actor_object_type",
    "objectType" -> "fgaa_object_type",
    "verbId" -> "fgaa_verb_id",
    "verbDisplay" -> "fgaa_verb_display",
    "objectId" -> "fgaa_object_id",
    "device" -> "fgaa_device",
    "schoolId" -> "school_uuid",
    "studentId" -> "student_uuid",
    "eventType" -> "fgaa_event_type",
    "timestampLocal" -> "fgaa_timestamp_local",
    "eventDateDw" -> "fgaa_date_dw_id",
    "occurredOn" -> "occurredOn"
  )

  lazy val SchoolAcademicYearDimensionCols = Map(
    "schoolId" -> "saya_school_id",
    "saya_status" -> "saya_status",
    "currentAcademicYearId" -> "saya_academic_year_id",
    "previousAcademicYearId" -> "saya_previous_academic_year_id",
    "saya_type" -> "saya_iwh_type",
    "sayaType" -> "saya_type",
    "occurredOn" -> "occurredOn"
  )

  lazy val SchoolDimensionCols = Map(
    "uuid" -> "school_id",
    "name" -> "school_name",
    "addressLine" -> "school_address_line",
    "addressPostBox" -> "school_post_box",
    "addressCity" -> "school_city_name",
    "addressCountry" -> "school_country_name",
    "latitude" -> "school_latitude",
    "longitude" -> "school_longitude",
    "firstTeachingDay" -> "school_first_day",
    "timeZone" -> "school_timezone",
    "school_status" -> "school_status",
    "composition" -> "school_composition",
    "occurredOn" -> "occurredOn",
    "tenantId" -> "school_tenant_id",
    "alias" -> "school_alias",
    "sourceId" -> "school_source_id",
    "contentRepositoryId" -> "school_content_repository_id",
    "organisationGlobal" -> "school_organization_code",
    "content_repository_dw_id" -> "school_content_repository_dw_id",
    "organization_dw_id" -> "school_organization_dw_id",
    "_is_complete" -> "_is_complete"
  )

  lazy val SchoolDimensionStatusCols = Map(
    "schoolId" -> "school_id",
    "eventType" -> "eventType",
    "school_status" -> "school_status",
    "occurredOn" -> "occurredOn"
  )

  val GradeDimensionCols = Map(
    "uuid" -> "grade_id",
    "name" -> "grade_name",
    "k12Grade" -> "grade_k12grade",
    "grade_status" -> "grade_status",
    "occurredOn" -> "occurredOn",
    "academicYearId" -> "academic_year_id",
    "tenantId" -> "tenant_id",
    "schoolId" -> "school_id"
  )

  lazy val SectionDimensionCols = Map(
    "uuid" -> "section_id",
    "enabled" -> "section_enabled",
    "section" -> "section_name",
    "name" -> "section_alias",
    "section_status" -> "section_status",
    "occurredOn" -> "occurredOn",
    "tenantId" -> "tenant_id",
    "schoolId" -> "school_id",
    "gradeId" -> "grade_id",
    "sourceId" -> "section_source_id"
  )

  val SectionDimensionDeltaCols: Map[String, String] = Map(
    "uuid" -> "section_id",
    "enabled" -> "section_enabled",
    "section" -> "section_name",
    "name" -> "section_alias",
    "section_status" -> "section_status",
    "tenantId" -> "section_tenant_id",
    "schoolId" -> "section_school_id",
    "gradeId" -> "section_grade_id",
    "sourceId" -> "section_source_id",
    "occurredOn" -> "occurredOn",
    "eventdate" -> "eventdate"
  )

  lazy val CurriculumCreatedDimCols = Map(
    "curr_dw_id" -> "curr_dw_id",
    "curriculumId" -> "curr_id",
    "name" -> "curr_name",
    "curr_status" -> "curr_status",
    "organisation" -> "curr_organisation",
    "occurredOn" -> "occurredOn"
  )

  lazy val CurriculumGradeCreatedDimCols = Map(
    "gradeId" -> "curr_grade_id",
    "name" -> "curr_grade_name",
    "curr_grade_status" -> "curr_grade_status",
    "occurredOn" -> "occurredOn"
  )

  lazy val CurriculumSbjMutatedDimCols = Map(
    "subjectId" -> "curr_subject_id",
    "name" -> "curr_subject_name",
    "curr_subject_status" -> "curr_subject_status",
    "skillable" -> "curr_subject_skillable",
    "occurredOn" -> "occurredOn"
  )

  lazy val CurriculumSbjDeletedDimCols =
    Map("subjectId" -> "curr_subject_id", "curr_subject_status" -> "curr_subject_status", "occurredOn" -> "occurredOn")

  lazy val OutcomeDimCols = Map("outcomeId" -> "outcome_id", "occurredOn" -> "occurredOn")

  lazy val OutcomeDimUpdatedCols = OutcomeDimCols ++ Map("parentId" -> "outcome_parent_id",
                                                         "name" -> "outcome_name",
                                                         "translations" -> "outcome_translations",
                                                         "description" -> "outcome_description")

  lazy val OutcomeCreatedDimCols = OutcomeDimCols ++ Map(
    "parentId" -> "outcome_parent_id",
    "outcomeType" -> "outcome_type",
    "name" -> "outcome_name",
    "description" -> "outcome_description",
    "boardId" -> "outcome_curriculum_id",
    "gradeId" -> "outcome_curriculum_grade_id",
    "subjectId" -> "outcome_curriculum_subject_id",
    "translations" -> "outcome_translations"
  )

  lazy val ContentAssociationBaseDimensionCols = Map(
    "contentId" -> "content_association_content_id",
    "content_association_status" -> "content_association_status",
    "content_association_type" -> "content_association_type",
    "content_association_attach_status" -> "content_association_attach_status",
    "occurredOn" -> "occurredOn"
  )

  lazy val ContentOutcomeDimensionCols = ContentAssociationBaseDimensionCols ++ Map("outcomeId" -> "content_association_id")
  lazy val ContentSkillDimensionCols = ContentAssociationBaseDimensionCols ++ Map("skillId" -> "content_association_id")

  lazy val ContentStudentDimensionCols = Map(
    "contentId" -> "content_student_association_step_id",
    "studentId" -> "content_student_association_student_id",
    "classId" -> "content_student_association_class_id",
    "mloId" -> "content_student_association_lo_id",
    "content_student_association_status" -> "content_student_association_status",
    "content_student_association_assign_status" -> "content_student_association_assign_status",
    "assignedBy" -> "content_student_association_assigned_by",
    "content_student_association_type" -> "content_student_association_type",
    "contentType" -> "content_student_association_content_type",
    "occurredOn" -> "occurredOn"
  )

  lazy val LessonAssignmentDimensionCols = Map(
    "studentId" -> "student_uuid",
    "classId" -> "class_uuid",
    "mloId" -> "lo_uuid",
    "assignedBy" -> "teacher_uuid",
    "lesson_assignment_status" -> "lesson_assignment_status",
    "lesson_assignment_attach_status" -> "lesson_assignment_assign_status",
    "lesson_assignment_type" -> "lesson_assignment_type",
    "occurredOn" -> "occurredOn"
  )

  lazy val ContentPublishedDimensionCols = Map(
    "contentId" -> "content_id",
    "publishedDate" -> "content_published_date",
    "publisherId" -> "content_publisher_id",
    "occurredOn" -> "occurredOn"
  )

  lazy val ContentDeletedDimensionCols =
    Map("contentId" -> "content_id", "content_status" -> "content_status", "occurredOn" -> "occurredOn")

  lazy val ContentCreatedDimensionCols = ContentDimensionCols ++ Map("createdAt" -> "content_created_at",
                                                                     "createdBy" -> "content_created_by",
                                                                     "content_status" -> "content_status")

  lazy val ContentDimensionCols = Map(
    "contentId" -> "content_id",
    "title" -> "content_title",
    "description" -> "content_description",
    "tags" -> "content_tags",
    "organisation" -> "content_organisation",
    "fileName" -> "content_file_name",
    "fileContentType" -> "content_file_content_type",
    "fileSize" -> "content_file_size",
    "fileUpdatedAt" -> "content_file_updated_at",
    "conditionOfUse" -> "content_condition_of_use",
    "knowledgeDimension" -> "content_knowledge_dimension",
    "difficultyLevel" -> "content_difficulty_level",
    "language" -> "content_language",
    "lexicalLevel" -> "content_lexical_level",
    "mediaType" -> "content_media_type",
    "status" -> "content_action_status",
    "contentLocation" -> "content_location",
    "authoredDate" -> "content_authored_date",
    "contentLearningResourceTypes" -> "content_learning_resource_types",
    "cognitiveDimensions" -> "content_cognitive_dimensions",
    "copyrights" -> "content_copyrights",
    "occurredOn" -> "occurredOn"
  )

  lazy val SectionStateChangedCols = Map(
    "uuid" -> "section_id",
    "section_status" -> "section_status",
    "occurredOn" -> "occurredOn",
    "section_enabled" -> "section_enabled"
  )

  lazy val SectionDeletedCols = Map(
    "uuid" -> "section_id",
    "section_status" -> "section_status",
    "occurredOn" -> "occurredOn"
  )

  lazy val GradeDeletedDimensionCols = Map(
    "uuid" -> "grade_id",
    "grade_status" -> "grade_status",
    "occurredOn" -> "occurredOn"
  )
  lazy val LoDimensionCols = Map(
    "id" -> "lo_id",
    "title" -> "lo_title",
    "code" -> "lo_code",
    "type" -> "lo_type",
    "lo_status" -> "lo_status",
    "curriculumId" -> "lo_curriculum_id",
    "curriculumSubjectId" -> "lo_curriculum_subject_id",
    "curriculumGradeId" -> "lo_curriculum_grade_id",
    "academicYear" -> "lo_content_academic_year",
    "order" -> "lo_order",
    "occurredOn" -> "occurredOn"
  )
  val CCLLessonBaseDimensionCols = Map(
    "lessonId" -> "lo_ccl_id",
    "title" -> "lo_title",
    "lessonType" -> "lo_type",
    "lo_status" -> "lo_status",
    "boardId" -> "lo_curriculum_id",
    "subjectId" -> "lo_curriculum_subject_id",
    "gradeId" -> "lo_curriculum_grade_id",
    "duration" -> "lo_duration",
    "occurredOn" -> "occurredOn",
    "mloFrameworkId" -> "lo_framework_id",
    "mloTemplateId" -> "lo_template_id",
    "userId" -> "lo_user_id",
    "skillable" -> "lo_skillable",
    "assessmentTool" -> "lo_assessment_tool",
    "organisation" -> "lo_organisation"
  )
  val CCLLessonCreateDimensionCols = CCLLessonBaseDimensionCols ++
    Map(
      "lo_action_status" -> "lo_action_status",
      "code" -> "lo_code",
      "lessonUuid" -> "lo_id",
      "academicYearId" -> "lo_content_academic_year_id",
      "academicYear" -> "lo_content_academic_year"
    )
  val CCLLessonUpdateDimensionCols = CCLLessonBaseDimensionCols
  val CCLLessonDeletedDimensionCols = Map(
    "lessonId" -> "lo_ccl_id",
    "lo_status" -> "lo_status",
    "occurredOn" -> "occurredOn"
  )
  val CCLLessonPublishedDimensionCols = Map(
    "lessonId" -> "lo_ccl_id",
    "maxStars" -> "lo_max_stars",
    "publisherId" -> "lo_publisher_id",
    "publishedDate" -> "lo_published_date",
    "lo_action_status" -> "lo_action_status",
    "occurredOn" -> "occurredOn"
  )
  val CCLLessonWorkFlowStatusChangedDimensionCols = Map(
    "lessonId" -> "lo_ccl_id",
    "lo_action_status" -> "lo_action_status",
    "occurredOn" -> "occurredOn"
  )
  val CCLLessonOutcomeAttachCols = Map(
    "lessonId" -> "lo_association_lo_ccl_id",
    "lessonUuid" -> "lo_association_lo_id",
    "outcomeId" -> "lo_association_id",
    "lo_association_status" -> "lo_association_status",
    "lo_association_attach_status" -> "lo_association_attach_status",
    "lo_association_type" -> "lo_association_type",
    "occurredOn" -> "occurredOn"
  )
  val CCLActivityOutcomeAttachCols = Map(
    "lessonId" -> "lo_association_lo_ccl_id",
    "activityUuid" -> "lo_association_lo_id",
    "outcomeId" -> "lo_association_id",
    "lo_association_status" -> "lo_association_status",
    "lo_association_attach_status" -> "lo_association_attach_status",
    "lo_association_type" -> "lo_association_type",
    "occurredOn" -> "occurredOn"
  )
  val CCLLessonContentAttachCols = Map(
    "lessonId" -> "step_instance_lo_ccl_id",
    "lessonUuid" -> "step_instance_lo_id",
    "contentId" -> "step_instance_id",
    "stepId" -> "step_instance_step_id",
    "stepUuid" -> "step_instance_step_uuid",
    "step_instance_status" -> "step_instance_status",
    "step_instance_attach_status" -> "step_instance_attach_status",
    "step_instance_type" -> "step_instance_type",
    "occurredOn" -> "occurredOn"
  )

  val CCLLessonAssignmentAttachCols = Map(
    "lessonId" -> "step_instance_lo_ccl_id",
    "lessonUuid" -> "step_instance_lo_id",
    "assignmentId" -> "step_instance_id",
    "stepId" -> "step_instance_step_id",
    "stepUuid" -> "step_instance_step_uuid",
    "step_instance_status" -> "step_instance_status",
    "step_instance_attach_status" -> "step_instance_attach_status",
    "step_instance_type" -> "step_instance_type",
    "occurredOn" -> "occurredOn"
  )

  val CCLLessonAssessmentRuleCols = Map(
    "lessonId" -> "step_instance_lo_ccl_id",
    "lessonUuid" -> "step_instance_lo_id",
    "stepId" -> "step_instance_step_id",
    "stepUuid" -> "step_instance_step_uuid",
    "rule.id" -> "step_instance_id",
    "rule.poolId" -> "step_instance_pool_id",
    "rule.poolName" -> "step_instance_pool_name",
    "rule.difficultyLevel" -> "step_instance_difficulty_level",
    "rule.resourceType" -> "step_instance_resource_type",
    "rule.questions" -> "step_instance_questions",
    "step_instance_status" -> "step_instance_status",
    "step_instance_attach_status" -> "step_instance_attach_status",
    "step_instance_type" -> "step_instance_type",
    "occurredOn" -> "occurredOn"
  )

  val CCLLessonSkillLinkCols = Map(
    "lessonId" -> "lo_association_lo_ccl_id",
    "lessonUuid" -> "lo_association_lo_id",
    "skillId" -> "lo_association_id",
    "derived" -> "lo_association_derived",
    "lo_association_status" -> "lo_association_status",
    "lo_association_attach_status" -> "lo_association_attach_status",
    "lo_association_type" -> "lo_association_type",
    "occurredOn" -> "occurredOn"
  )

  val CCLActivityTemplateCols: Map[String, String] = Map(
    "description" -> "at_description",
    "activityType" -> "at_activity_type",
    "publisherId" -> "at_publisher_id",
    "publisherName" -> "at_publisher_name",
    "publishedDate" -> "at_published_date",
    "dateCreated" -> "occurredOn",
    "at_status" -> "at_status"
  )

  val CCLActivityTemplateRedshiftCols: Map[String, String] = CCLActivityTemplateCols ++ Map(
    "templateUuid" -> "at_uuid",
    "activityTemplateName" -> "at_name",
    "id" -> "at_component_uuid",
    "order" -> "at_order",
    "name" -> "at_component_name",
    "abbreviation" -> "at_abbreviation",
    "icon" -> "at_icon",
    "maxRepeat" -> "at_max_repeat",
    "exitTicket" -> "at_exit_ticket",
    "completionNode" -> "at_completion_node",
    "alwaysEnabled" -> "at_always_enabled",
    "passingScore" -> "at_passing_score",
    "assessmentsAttempts" -> "at_assessments_attempts",
    "hints" -> "at_question_attempts_hints",
    "attempts" -> "at_question_attempts",
    "releaseCondition" -> "at_release_condition",
    "sectionType" -> "at_section_type",
    "releaseComponent" -> "at_release_component",
    "min" -> "at_min",
    "max" -> "at_max",
    "at_component_type" -> "at_component_type",
  )

  val CCLActivityTemplateDeltaCols: Map[String, String] = CCLActivityTemplateCols ++ Map(
    "templateUuid" -> "at_id",
    "name" -> "at_name",
    "mainComponents" -> "mainComponents",
    "supportingComponents" -> "supportingComponents",
    "sideComponents" -> "sideComponents"
  )

  val CCLActivityBaseDimensionCols = Map(
    "lessonId" -> "lo_ccl_id",
    "code" -> "lo_code",
    "activityUuid" -> "lo_id",
    "title" -> "lo_title",
    "activityType" -> "lo_type",
    "lo_status" -> "lo_status",
    "boardId" -> "lo_curriculum_id",
    "subjectId" -> "lo_curriculum_subject_id",
    "gradeId" -> "lo_curriculum_grade_id",
    "duration" -> "lo_duration",
    "occurredOn" -> "occurredOn",
    "skillable" -> "lo_skillable",
    "maxStars" -> "lo_max_stars",
    "lo_action_status" -> "lo_action_status",
    "academicYearId" -> "lo_content_academic_year_id",
    "academicYear" -> "lo_content_academic_year",
    "language" -> "lo_language"
  )

  val CCLActivityCreateUpdateExtraCols = Map(
    "userId" -> "lo_user_id",
    "organisation" -> "lo_organisation",
  )

  val CCLActivityCreatedDimensionCols = CCLActivityBaseDimensionCols ++ CCLActivityCreateUpdateExtraCols

  val CCLActivityUpdatedDimensionCols = CCLActivityBaseDimensionCols ++ CCLActivityCreateUpdateExtraCols

  val CCLActionPublishedDimensionCols = CCLActivityBaseDimensionCols ++ Map(
    "publisherId" -> "lo_publisher_id",
    "publishedDate" -> "lo_published_date",
    "activityTemplateUuid" -> "lo_template_uuid"
  )

  val CCLActivityDeletedDimensionCols = Map(
    "lessonId" -> "lo_ccl_id",
    "lo_status" -> "lo_status",
    "occurredOn" -> "occurredOn"
  )

  val CCLActivityWorkFlowStatusChangedDimensionCols = Map(
    "lessonId" -> "lo_ccl_id",
    "lo_action_status" -> "lo_action_status",
    "occurredOn" -> "occurredOn"
  )

  val CCLActivityComponentsCols = Map(
    "componentStepUuid" -> "step_instance_step_uuid",
    "lessonId" -> "step_instance_lo_ccl_id",
    "activityUuid" -> "step_instance_lo_id",
    "componentId" -> "step_instance_id",
    "step_instance_status" -> "step_instance_status",
    "step_instance_attach_status" -> "step_instance_attach_status",
    "step_instance_type" -> "step_instance_type",
    "occurredOn" -> "occurredOn",
    "componentActivityTemplateComponentId" -> "step_instance_template_uuid",
    "componentTitle" -> "step_instance_title",
    "componentMediaType" -> "step_instance_media_type",
    "poolId" -> "step_instance_pool_id",
    "poolName" -> "step_instance_pool_name",
    "difficultyLevel" -> "step_instance_difficulty_level",
    "resourceType" -> "step_instance_resource_type",
    "questions" -> "step_instance_questions"
  )

  lazy val LearningPathDimensionCols = Map(
    "composite_id" -> "learning_path_id",
    "id" -> "learning_path_uuid",
    "schoolId" -> "learning_path_school_id",
    "name" -> "learning_path_name",
    "status" -> "learning_path_lp_status",
    "languageTypeScript" -> "learning_path_language_type_script",
    "experientialLearning" -> "learning_path_experiential_learning",
    "tutorDhabiEnabled" -> "learning_path_tutor_dhabi_enabled",
    "default" -> "learning_path_default",
    "curriculumId" -> "learning_path_curriculum_id",
    "curriculumGradeId" -> "learning_path_curriculum_grade_id",
    "curriculumSubjectId" -> "learning_path_curriculum_subject_id",
    "schoolSubjectId" -> "learning_path_subject_id",
    "learning_path_status" -> "learning_path_status",
    "academicYearId" -> "learning_path_academic_year_id",
    "academicYear" -> "learning_path_content_academic_year",
    "classId" -> "learning_path_class_id",
    "occurredOn" -> "occurredOn"
  )

  lazy val TeacherDimensionCols = Map(
    "schoolTeacherUuid" -> "teacher_id",
    "schoolId" -> "school_id",
    "occurredOn" -> "occurredOn",
    "teacher_status" -> "teacher_status",
    "teacher_active_until" -> "teacher_active_until"
  )

  lazy val GuardianDimensionCols = Map(
    "guardianId" -> "guardian_id",
    "studentId" -> "student_id",
    "occurredOn" -> "occurredOn",
    "status" -> "guardian_status",
    "invitationStatus" -> "guardian_invitation_status"
  )

  lazy val StudentDimensionCols = Map(
    "uuid" -> "student_uuid",
    "username" -> "student_username",
    "schoolId" -> "school_uuid",
    "gradeId" -> "grade_uuid",
    "sectionId" -> "section_uuid",
    "student_status" -> "student_status",
    "tags" -> "student_tags",
    "specialNeeds" -> "student_special_needs",
    "occurredOn" -> "occurredOn"
  )

  lazy val AdminUserDimensionCols = Map(
    "uuid" -> "admin_uuid",
    "onboarded" -> "admin_onboarded",
    "school_uuid" -> "school_uuid",
    "expirable" -> "admin_expirable",
    "avatar" -> "admin_avatar",
    "occurredOn" -> "occurredOn",
    "admin_original_status" -> "admin_status",
    "role" -> "role_uuid"
  )

  lazy val UserDeletedDimensionCols = Map(
    "uuid" -> "admin_uuid",
    "occurredOn" -> "occurredOn",
  )

  lazy val AcademicYearStartedCols = Map(
    "id" -> s"${AcademicYearEntity}_id",
    "schoolId" -> s"${AcademicYearEntity}_school_id",
    "startDate" -> s"${AcademicYearEntity}_start_date",
    "endDate" -> s"${AcademicYearEntity}_end_date",
    s"${AcademicYearEntity}_status" -> s"${AcademicYearEntity}_status",
    s"${AcademicYearEntity}_is_roll_over_completed" -> s"${AcademicYearEntity}_is_roll_over_completed",
    s"${AcademicYearEntity}_organization_code" -> s"${AcademicYearEntity}_organization_code",
    s"${AcademicYearEntity}_created_by" -> s"${AcademicYearEntity}_created_by",
    s"${AcademicYearEntity}_updated_by" -> s"${AcademicYearEntity}_updated_by",
    s"${AcademicYearEntity}_state" -> s"${AcademicYearEntity}_state",
    s"${AcademicYearEntity}_type" -> s"${AcademicYearEntity}_type",
    "occurredOn" -> "occurredOn"
  )
  lazy val AcademicYearUpdatedCols = Map(
    "id" -> s"${AcademicYearEntity}_id",
    "schoolId" -> s"${AcademicYearEntity}_school_id",
    "startDate" -> s"${AcademicYearEntity}_start_date",
    "endDate" -> s"${AcademicYearEntity}_end_date",
    s"${AcademicYearEntity}_status" -> s"${AcademicYearEntity}_status",
    s"${AcademicYearEntity}_organization_code" -> s"${AcademicYearEntity}_organization_code",
    s"${AcademicYearEntity}_created_by" -> s"${AcademicYearEntity}_created_by",
    s"${AcademicYearEntity}_updated_by" -> s"${AcademicYearEntity}_updated_by",
    s"${AcademicYearEntity}_state" -> s"${AcademicYearEntity}_state",
    s"${AcademicYearEntity}_type" -> s"${AcademicYearEntity}_type",
    "occurredOn" -> "occurredOn"
  )
  lazy val AcademicYearRollOverCompletedCols = Map(
    "schoolId" -> s"${AcademicYearEntity}_school_id",
    "previousId" -> s"${AcademicYearEntity}_id",  //note: mapping previousId to id because delta sink adds redundant previous_id column
    s"${AcademicYearEntity}_is_roll_over_completed" -> s"${AcademicYearEntity}_is_roll_over_completed",
    s"${AcademicYearEntity}_status" -> s"${AcademicYearEntity}_status",
    "occurredOn" -> "occurredOn"
  )

  val IncGameCols = Map(
    "gameId" -> "inc_game_id",
    "eventType" -> "inc_game_event_type",
    "occurredOn" -> "occurredOn",
    "inc_game_date_dw_id" -> "inc_game_date_dw_id",
    "tenantId" -> "tenant_uuid",
    "schoolId" -> "school_uuid",
    "sectionId" -> "section_uuid",
    "mloId" -> "lo_uuid",
    "title" -> "inc_game_title",
    "teacherId" -> "teacher_uuid",
    "subjectId" -> "subject_uuid",
    "schoolGradeId" -> "grade_uuid",
    "numQuestions" -> "inc_game_num_questions",
    "instructionalPlanId" -> "inc_game_instructional_plan_id",
    "classId" -> "class_uuid",
    "isFormativeAssessment" -> "inc_game_is_assessment",
    "lessonComponentId" -> "inc_game_lesson_component_id"
  )

  val IncGameDeltaCols = Map(
    "inc_game_id" -> "inc_game_id",
    "inc_game_event_type" -> "inc_game_event_type",
    "tenant_uuid" -> "inc_game_tenant_id",
    "school_uuid" -> "inc_game_school_id",
    "section_uuid" -> "inc_game_section_id",
    "lo_uuid" -> "inc_game_lo_id",
    "inc_game_title" -> "inc_game_title",
    "teacher_uuid" -> "inc_game_teacher_id",
    "subject_uuid" -> "inc_game_subject_id",
    "grade_uuid" -> "inc_game_grade_id",
    "inc_game_num_questions" -> "inc_game_num_questions",
    "inc_game_is_assessment" -> "inc_game_is_assessment",
    "inc_game_lesson_component_id" -> "inc_game_lesson_component_id",
    "inc_game_instructional_plan_id" -> "inc_game_instructional_plan_id",
    "class_uuid" -> "inc_game_class_id",
    "inc_game_created_time" -> "inc_game_created_time",
    "inc_game_dw_created_time" -> "inc_game_dw_created_time",
    "inc_game_date_dw_id" -> "inc_game_date_dw_id",
    "inc_game_updated_time" -> "inc_game_updated_time",
    "inc_game_dw_updated_time" -> "inc_game_dw_updated_time"
  )

  val IncGameSessionCols = Map(
    "sessionId" -> "inc_game_session_id",
    "occurredOn" -> "occurredOn",
    "inc_game_session_date_dw_id" -> "inc_game_session_date_dw_id",
    "tenantId" -> "tenant_uuid",
    "gameId" -> "game_uuid",
    "gameTitle" -> "inc_game_session_title",
    "startedBy" -> "inc_game_session_started_by",
    "status" -> "inc_game_session_status",
    "numPlayers" -> "inc_game_session_num_players",
    "numJoinedPlayers" -> "inc_game_session_num_joined_players",
    "joinedPlayersDetails" -> "inc_game_session_joined_players_details",
    "isStart" -> "inc_game_session_is_start",
    "isStartProcessed" -> "inc_game_session_is_start_event_processed",
    "isFormativeAssessment" -> "inc_game_session_is_assessment"
  )

  val IncGameSessionDeltaCols = Map(
    "inc_game_session_id" -> "inc_game_session_id",
    "inc_game_session_created_time" -> "inc_game_session_created_time",
    "inc_game_session_dw_created_time" -> "inc_game_session_dw_created_time",
    "inc_game_session_date_dw_id" -> "inc_game_session_date_dw_id",
    "tenant_uuid" -> "inc_game_session_tenant_id",
    "game_uuid" -> "inc_game_session_game_id",
    "inc_game_session_title" -> "inc_game_session_title",
    "inc_game_session_started_by" -> "inc_game_session_started_by",
    "inc_game_session_status" -> "inc_game_session_status",
    "inc_game_session_num_players" -> "inc_game_session_num_players",
    "inc_game_session_num_joined_players" -> "inc_game_session_num_joined_players",
    "inc_game_session_joined_players_details" -> "inc_game_session_joined_players_details",
    "inc_game_session_is_start" -> "inc_game_session_is_start",
    "inc_game_session_is_start_event_processed" -> "inc_game_session_is_start_event_processed",
    "inc_game_session_is_assessment" -> "inc_game_session_is_assessment"
  )

  val IncGameOutcomeCols = Map(
    "outcomeId" -> "inc_game_outcome_id",
    "occurredOn" -> "occurredOn",
    "inc_game_outcome_date_dw_id" -> "inc_game_outcome_date_dw_id",
    "tenantId" -> "tenant_uuid",
    "sessionId" -> "session_uuid",
    "gameId" -> "game_uuid",
    "playerId" -> "player_uuid",
    "mloId" -> "lo_uuid",
    "score" -> "inc_game_outcome_score",
    "status" -> "inc_game_outcome_status",
    "scoreBreakdown" -> "scoreBreakdown",
    "isFormativeAssessment" -> "inc_game_outcome_is_assessment"
  )

  val IncGameOutcomeDeltaCols = Map(
    "inc_game_outcome_id" -> "inc_game_outcome_id",
    "tenant_uuid" -> "inc_game_outcome_tenant_id",
    "session_uuid" -> "inc_game_outcome_session_id",
    "game_uuid" -> "inc_game_outcome_game_id",
    "player_uuid" -> "inc_game_outcome_player_id",
    "lo_uuid" -> "inc_game_outcome_lo_id",
    "inc_game_outcome_score" -> "inc_game_outcome_score",
    "inc_game_outcome_status" -> "inc_game_outcome_status",
    "inc_game_outcome_created_time" -> "inc_game_outcome_created_time",
    "inc_game_outcome_dw_created_time" -> "inc_game_outcome_dw_created_time",
    "inc_game_outcome_date_dw_id" -> "inc_game_outcome_date_dw_id",
    "scoreBreakdown" -> "inc_game_outcome_score_breakdown",
    "inc_game_outcome_is_assessment" -> "inc_game_outcome_is_assessment"
  )

  lazy val curioStudent = Map(
    "adek_id" -> "adek_id",
    "status" -> "status",
    "interaction_type" -> "interaction_type",
    "timestamp" -> "time_stamp",
    "topic_name" -> "topic_name",
    "subject_id" -> "subject_id",
    "role" -> "role",
    "eventDateDw" -> "date_dw_id",
    "loadtime" -> "loadtime"
  )

  lazy val curioTeacher = Map(
    "student" -> "student_id",
    "teacher" -> "teacher_id",
    "topic" -> "topic",
    "createdat" -> "created_at",
    "updatedat" -> "updated_at",
    "eventDateDw" -> "date_dw_id",
    "loadtime" -> "loadtime"
  )

  lazy val StagingLessonFeedback = Map(
    "tenantId" -> "tenant_uuid",
    "eventDateDw" -> "lesson_feedback_date_dw_id",
    "feedbackId" -> "lesson_feedback_id",
    "schoolId" -> "school_uuid",
    "academicYearId" -> "academic_year_uuid",
    "classId" -> "class_uuid",
    "gradeId" -> "grade_uuid",
    "section" -> "section_uuid",
    "subjectId" -> "subject_uuid",
    "trimesterId" -> "lesson_feedback_trimester_id",
    "trimesterOrder" -> "lesson_feedback_trimester_order",
    "learnerId" -> "student_uuid",
    "learningSessionId" -> "fle_ls_uuid",
    "learningObjectiveId" -> "lo_uuid",
    "curriculumId" -> "curr_uuid",
    "curriculumGradeId" -> "curr_grade_uuid",
    "curriculumSubjectId" -> "curr_subject_uuid",
    "contentAcademicYear" -> "lesson_feedback_content_academic_year",
    "rating" -> "lesson_feedback_rating",
    "ratingText" -> "lesson_feedback_rating_text",
    "hasComment" -> "lesson_feedback_has_comment",
    "instructionalPlanId" -> "lesson_feedback_instructional_plan_id",
    "learningPathId" -> "lesson_feedback_learning_path_id",
    "teachingPeriodId" -> "lesson_feedback_teaching_period_id",
    "teachingPeriodTitle" -> "lesson_feedback_teaching_period_title",
    "isCancelled" -> "lesson_feedback_is_cancelled",
    "occurredOn" -> "occurredOn"
  )

  lazy val InstructionPlanCols = Map(
    "id" -> "instructional_plan_id",
    "name" -> "instructional_plan_name",
    "curriculumId" -> "instructional_plan_curriculum_id",
    "subjectId" -> "instructional_plan_curriculum_subject_id",
    "gradeId" -> "instructional_plan_curriculum_grade_id",
    "academicYearId" -> "instructional_plan_content_academic_year_id",
    "instructional_plan_status" -> "instructional_plan_status",
    "contentRepositoryId" -> "instructional_plan_content_repository_id",
    "contentRepositoryUuid" -> "content_repository_uuid",
    "items.itemType" -> "instructional_plan_item_type",
    "items.checkpointUuid" -> "ic_uuid",
    "items.lessonUuid" -> "lo_uuid",
    "items.order" -> "instructional_plan_item_order",
    "items.weekId" -> "week_uuid",
    "items.lessonId" -> "instructional_plan_item_ccl_lo_id",
    "items.optional" -> "instructional_plan_item_optional",
    "items.instructorLed" -> "instructional_plan_item_instructor_led",
    "items.defaultLocked" -> "instructional_plan_item_default_locked",
    "occurredOn" -> "occurredOn"
  )

  lazy val InstructionPlanDeletedCols = Map(
    "id" -> "instructional_plan_id",
    "instructional_plan_status" -> "instructional_plan_status",
    "occurredOn" -> "occurredOn"
  )

  lazy val ClassDimensionCols = Map(
    "classId" -> "class_id",
    "title" -> "class_title",
    "schoolId" -> "class_school_id",
    "gradeId" -> "class_grade_id",
    "sectionId" -> "class_section_id",
    "academicYearId" -> "class_academic_year_id",
    "academicCalendarId" -> "class_academic_calendar_id",
    "subjectCategory" -> "class_gen_subject",
    "material.curriculum" -> "class_curriculum_id",
    "material.grade" -> "class_curriculum_grade_id",
    "material.subject" -> "class_curriculum_subject_id",
    "material.year" -> "class_content_academic_year",
    "material.instructionalPlanId" -> "class_curriculum_instructional_plan_id",
    "material.materialId" -> "class_material_id",
    "material.materialType" -> "class_material_type",
    "settings.tutorDhabiEnabled" -> "class_tutor_dhabi_enabled",
    "settings.languageDirection" -> "class_language_direction",
    "settings.online" -> "class_online",
    "settings.practice" -> "class_practice",
    "status" -> "class_course_status",
    "sourceId" -> "class_source_id",
    "class_status" -> "class_status",
    "occurredOn" -> "occurredOn",
    "categoryId" -> "class_category_id"
  )

  lazy val ClassDeletedCols = Map(
    "classId" -> "class_id",
    "class_academic_year_id" -> "class_academic_year_id",
    "class_academic_calendar_id" -> "class_academic_calendar_id",
    "class_category_id" -> "class_category_id",
    "class_content_academic_year" -> "class_content_academic_year",
    "class_course_status" -> "class_course_status",
    "class_curriculum_grade_id" -> "class_curriculum_grade_id",
    "class_curriculum_id" -> "class_curriculum_id",
    "class_curriculum_instructional_plan_id" -> "class_curriculum_instructional_plan_id",
    "class_curriculum_subject_id" -> "class_curriculum_subject_id",
    "class_material_id" -> "class_material_id",
    "class_material_type" -> "class_material_type",
    "class_gen_subject" -> "class_gen_subject",
    "class_grade_id" -> "class_grade_id",
    "class_language_direction" -> "class_language_direction",
    "class_online" -> "class_online",
    "class_practice" -> "class_practice",
    "class_school_id" -> "class_school_id",
    "class_section_id" -> "class_section_id",
    "class_source_id" -> "class_source_id",
    "class_title" -> "class_title",
    "class_tutor_dhabi_enabled" -> "class_tutor_dhabi_enabled",
    "class_status" -> "class_status",
    "occurredOn" -> "occurredOn"
  )

  lazy val ClassUserCols = Map(
    "classId" -> "class_uuid",
    "userId" -> "user_uuid",
    "role" -> "role_uuid",
    "action" -> "action",
    "eventType" -> "eventType",
    "class_user_status" -> "class_user_status",
    "occurredOn" -> "occurredOn"
  )

  val CCLSkillColumns: Map[String, String] = Map(
    "id" -> "ccl_skill_id",
    "code" -> "ccl_skill_code",
    "name" -> "ccl_skill_name",
    "description" -> "ccl_skill_description",
    "translations" -> "ccl_skill_translations",
    "subjectId" -> "ccl_skill_subject_id",
    "ccl_skill_status" -> "ccl_skill_status",
    "occurredOn" -> "occurredOn"
  )

  val CCLSkillDeletedColumns: Map[String, String] = Map(
    "id" -> "ccl_skill_id",
    "ccl_skill_status" -> "ccl_skill_status",
    "occurredOn" -> "occurredOn"
  )

  val SkillAssociationColumns: Map[String, String] = Map(
    "skillId" -> "skill_association_skill_id",
    "skillCode" -> "skill_association_skill_code",
    "skill_association_status" -> "skill_association_status",
    "skill_association_attach_status" -> "skill_association_attach_status",
    "skill_association_type" -> "skill_association_type",
    "skill_association_id" -> "skill_association_id",
    "skill_association_code" -> "skill_association_code",
    "skill_association_is_previous" -> "skill_association_is_previous",
    "occurredOn" -> "occurredOn"
  )

  val OutcomeCategoryAssociationCols = Map(
    "outcomeId" -> "outcome_association_outcome_id",
    "categoryId" -> "outcome_association_id",
    "outcome_association_status" -> "outcome_association_status",
    "outcome_association_type" -> "outcome_association_type",
    "outcome_association_attach_status" -> "outcome_association_attach_status",
    "occurredOn" -> "occurredOn"
  )

  val OutcomeSkillAssociationCols = Map(
    "outcomeId" -> "outcome_association_outcome_id",
    "skillId" -> "outcome_association_id",
    "outcome_association_status" -> "outcome_association_status",
    "outcome_association_type" -> "outcome_association_type",
    "outcome_association_attach_status" -> "outcome_association_attach_status",
    "occurredOn" -> "occurredOn"
  )

  val AssignmentInstanceCreatedColumns = Map(
    "tenantId" -> "assignment_instance_tenant_id",
    "id" -> "assignment_instance_id",
    "assignmentId" -> "assignment_instance_assignment_id",
    "dueOn" -> "assignment_instance_due_on",
    "allowLateSubmission" -> "assignment_instance_allow_late_submission",
    "assignment_instance_status" -> "assignment_instance_status",
    "teacherId" -> "assignment_instance_teacher_id",
    "type" -> "assignment_instance_type",
    "schoolGradeId" -> "assignment_instance_grade_id",
    "classId" -> "assignment_instance_class_id",
    "subjectId" -> "assignment_instance_subject_id",
    "sectionId" -> "assignment_instance_section_id",
    "levelId" -> "assignment_instance_learning_path_id",
    "mloId" -> "assignment_instance_lo_id",
    "startOn" -> "assignment_instance_start_on",
    "trimesterId" -> "assignment_instance_trimester_id",
    "groupId" -> "assignment_instance_group_id",
    "instructionalPlanId" -> "assignment_instance_instructional_plan_id",
    "teachingPeriodId" -> "assignment_instance_teaching_period_id",
    "occurredOn" -> "occurredOn"
  )

  val AssignmentInstanceUpdatedColumns = Map(
    "id" -> "assignment_instance_id",
    "dueOn" -> "assignment_instance_due_on",
    "allowLateSubmission" -> "assignment_instance_allow_late_submission",
    "assignment_instance_status" -> "assignment_instance_status",
    "type" -> "assignment_instance_type",
    "startOn" -> "assignment_instance_start_on",
    "teachingPeriodId" -> "assignment_instance_teaching_period_id",
    "occurredOn" -> "occurredOn"
  )

  val AssignmentInstanceDeletedColumns = Map(
    "id" -> "assignment_instance_id",
    "assignment_instance_status" -> "assignment_instance_status",
    "occurredOn" -> "occurredOn"
  )

  val AssignmentInstanceStudentColumns = Map(
    "id" -> "ais_instance_id",
    "studentId" -> "ais_student_id",
    "ais_status" -> "ais_status",
    "occurredOn" -> "occurredOn"
  )

  val AssignmentSubmissionColumns = Map(
    "comment" -> "assignment_submission_student_comment",
    "id" -> "assignment_submission_id",
    "assignmentId" -> "assignment_submission_assignment_id",
    "assignmentInstanceId" -> "assignment_submission_assignment_instance_id",
    "referrerId" -> "assignment_submission_referrer_id",
    "type" -> "assignment_submission_type",
    "updatedOn" -> "assignment_submission_updated_on",
    "returnedOn" -> "assignment_submission_returned_on",
    "submittedOn" -> "assignment_submission_submitted_on",
    "gradedOn" -> "assignment_submission_graded_on",
    "evaluatedOn" -> "assignment_submission_evaluated_on",
    "status" -> "assignment_submission_status",
    "studentId" -> "assignment_submission_student_id",
    "teacherId" -> "assignment_submission_teacher_id",
    "studentAttachment.fileName" -> "assignment_submission_student_attachment_file_name",
    "studentAttachment.path" -> "assignment_submission_student_attachment_path",
    "teacherResponse.attachment.fileName" -> "assignment_submission_teacher_attachment_file_name",
    "teacherResponse.attachment.path" -> "assignment_submission_teacher_attachment_path",
    "teacherResponse.comment" -> "assignment_submission_teacher_comment",
    "teacherResponse.score" -> "assignment_submission_teacher_score",
    "tenantId" -> "assignment_submission_tenant_id",
    "resubmissionCount" -> "assignment_submission_resubmission_count",
    "occurredOn" -> "occurredOn"
  )
  val AssignmentMutatedColumns = Map(
    "id" -> "assignment_id",
    "type" -> "assignment_type",
    "title" -> "assignment_title",
    "description" -> "assignment_description",
    "maxScore" -> "assignment_max_score",
    "attachment.fileId" -> "assignment_attachment_file_id",
    "attachment.fileName" -> "assignment_attachment_file_name",
    "attachment.path" -> "assignment_attachment_path",
    "tenantId" -> "assignment_tenant_id",
    "schoolId" -> "assignment_school_id",
    "isGradeable" -> "assignment_is_gradeable",
    "language" -> "assignment_language",
    "status" -> "assignment_assignment_status",
    "assignment_status" -> "assignment_status",
    "attachmentRequired" -> "assignment_attachment_required",
    "commentRequired" -> "assignment_comment_required",
    "createdBy" -> "assignment_created_by",
    "updatedBy" -> "assignment_updated_by",
    "publishedOn" -> "assignment_published_on",
    "occurredOn" -> "occurredOn"
  )

  val AssignmentUpdatedColumns = AssignmentMutatedColumns -- List("tenantId", "schoolId")

  val AlefAssignmentMutatedColumns = AssignmentMutatedColumns ++ Map(
    "metadata.keywords" -> "assignment_metadata_keywords",
    "metadata.resourceType" -> "assignment_metadata_resource_type",
    "metadata.summativeAssessment" -> "assignment_metadata_is_sa",
    "metadata.difficultyLevel" -> "assignment_metadata_difficulty_level", //
    "metadata.cognitiveDimensions" -> "assignment_metadata_cognitive_dimensions",
    "metadata.knowledgeDimensions" -> "assignment_metadata_knowledge_dimensions", //
    "metadata.lexileLevel" -> "assignment_metadata_lexile_level", //
    "metadata.copyrights" -> "assignment_metadata_copyrights",
    "metadata.conditionsOfUse" -> "assignment_metadata_conditions_of_use",
    "metadata.formatType" -> "assignment_metadata_format_type", //
    "metadata.author" -> "assignment_metadata_author", //
    "metadata.authoredDate" -> "assignment_metadata_authored_date", //
    "metadata.curriculumOutcomes" -> "assignment_metadata_curriculum_outcomes",
    "metadata.skillIds" -> "assignment_metadata_skills",
    "metadata.language" -> "assignment_metadata_language" //
  ) -- List("tenantId")

  val AlefAssignmentUpdatedColumns = AlefAssignmentMutatedColumns -- List("tenantId", "schoolId")

  val AssignmentDeletedColumns = Map(
    "id" -> "assignment_id",
    "assignment_status" -> "assignment_status",
    "occurredOn" -> "occurredOn"
  )

  val ClassScheduleCols = Map(
    "classId" -> "class_schedule_class_id",
    "class_schedule_status" -> "class_schedule_status",
    "class_schedule_attach_status" -> "class_schedule_attach_status",
    "day" -> "class_schedule_day",
    "startTime" -> "class_schedule_start_time",
    "endTime" -> "class_schedule_end_time",
    "occurredOn" -> "occurredOn"
  )

  // CX Plc Info Dimension
  val CxPlcInfoDimension = "cx-plc-info-dimension"
  val CxPlcInfoParquetSource = "parquet-cx-plc-info-source"
  val CxPlcInfoRedshiftSink = "redshift-plc-info-sink"
  val CxPlcInfoParquetSink = "parquet-cx-plc-info-sink"
  val CxPlcInfoDeltaSink = "delta-plc-info-sink"
  val PLC_InfoCreatedEvent = "PLC_InfoCreatedEvent"
  val PLC_InfoUpdatedEvent = "PLC_InfoUpdatedEvent"
  val PLC_InfoDeletedEvent = "PLC_InfoDeletedEvent"
  val plcEntity = "cx_plc_info"
  val cxPlcInfoCols = Map(
    "id" -> "cx_plc_info_id",
    "plc_code" -> "cx_plc_info_code",
    "term" -> "cx_plc_info_term",
    "acc_year" -> "cx_plc_info_acc_year",
    "title" -> "cx_plc_info_title",
    "audience" -> "cx_plc_info_audience",
    "status" -> "cx_plc_info_cx_status",
    "occurredOn" -> "occurredOn",
    "creation_date" -> "cx_plc_info_creation_date",
    "cx_plc_info_status" -> "cx_plc_info_status"
  )

  // CX Role Dimension
  val CxRoleDimensionName = "cx-role-dimension"
  val CxRolesParquetSource = "parquet-cx-roles-source"

  val CxRoleParquetSink  = "parquet-cx-role-sink"

  val CxRoleCreatedRedshiftSink  = "redshift-cx-role-created-sink"
  val CxRoleUpdatedRedshiftSink  = "redshift-cx-role-updated-sink"
  val CxRoleDeletedRedshiftSink  = "redshift-cx-role-deleted-sink"

  val CxRoleCreatedDeltaSink  = "delta-cx-role-created-sink"
  val CxRoleUpdatedDeltaSink  = "delta-cx-role-updated-sink"
  val CxRoleDeletedDeltaSink  = "delta-cx-role-deleted-sink"

  val CxRoleEntityPrefix = "role"
  val cxRoleCols = Map(
    "id" -> "role_id",
    "role" -> "role_title",
    "role_status" -> "role_status",
    "occurredOn" -> "occurredOn"
  )

  //CX User Dimension
  val StagingEventType = "staging"

  val CxUserDimensionName = "cx-user-dimension"
  val CxUserEntityPrefix = "cx_user"

  val CxUserParquetSource = "parquet-cx-users-source"
  val CxSchoolUserParquetSource = "parquet-cx-school-user-source"

  val CxStagingUserUpsertedParquet = "parquet-stage-cx-users-upserted"
  val CxStagingUserDeletedParquet = "parquet-stage-cx-users-deleted"

  val CxStagingSchoolUserCreatedParquet = "parquet-stage-cx-school-user-created"
  val CxStagingSchoolUserUpdatedParquet = "parquet-stage-cx-school-user-updated"

  val CxUserParquetSink  = "parquet-cx-user-sink"
  val CxSchoolUserParquetSink  = "parquet-cx-school-user-sink"

  var CxUserCreatedRedshiftSink = "redshift-cx-user-created-sink"
  var CxUserUpdatedRedshiftSink = "redshift-cx-user-updated-sink"
  var CxUserDeletedRedshiftSink = "redshift-cx-user-deleted-sink"

  var CxUserCreatedDeltaSink = "delta-cx-user-created-sink"
  var CxUserUpdatedDeltaSink = "delta-cx-user-updated-sink"
  var CxUserDeletedDeltaSink = "delta-cx-user-deleted-sink"


  val cxUserCommonCols: Map[String, String] = Map(
    "id" -> "cx_user_cx_id",
    "uuid" -> "cx_user_id",
    "cx_user_status" -> "cx_user_status",
    "subject" -> "cx_user_subject",
    "status" -> "cx_user_cx_status",
  )

  val cxUserCreatedRedshiftCols: Map[String, String] = cxUserCommonCols ++ Map(
    "user_dw_id" -> "cx_user_dw_id",
    "role_dw_id" -> "cx_user_role_dw_id",
  )

  val cxUserCreatedDeltaCols: Map[String, String] = cxUserCommonCols ++ Map(
    "role_id" -> "cx_user_role_id",
  )

  val cxUserCreatedCols: Map[String, String] = cxUserCreatedRedshiftCols ++ cxUserCreatedDeltaCols ++ Map("occurredOn" -> "occurredOn")

  val cxUserCommonDatetimeCols = Seq(
    "cx_user_created_time",
    "cx_user_dw_created_time",
    "cx_user_updated_time",
    "cx_user_deleted_time",
    "cx_user_dw_updated_time",
  )

  val cxUserCreatedDtRedshiftCols: Seq[String] =  cxUserCreatedRedshiftCols.values.toSeq ++ cxUserCommonDatetimeCols
  val cxUserCreatedDtDeltaCols: Seq[String] =  cxUserCreatedDeltaCols.values.toSeq ++ cxUserCommonDatetimeCols

  val cxUserUpdatedCols: Map[String, String] = Map(
    "id" -> "cx_user_cx_id",
    "uuid" -> "cx_user_id",
    "occurredOn" -> "occurredOn",
    "subject" -> "cx_user_subject",
    "status" -> "cx_user_cx_status",
  )

  val cxUserDeletedCols: Map[String, String] = Map(
    "id" -> "cx_user_cx_id",
    "cx_user_status" -> "cx_user_status",
    "occurredOn" -> "occurredOn",
  )

  // Cx School User (Cx User Dimension)
  val cxSchoolUserCommonCols: Map[String, String] = Map(
    "user_id" -> "cx_user_cx_id",
  )

  val cxSchoolUserRedshiftCols: Map[String, String] = cxSchoolUserCommonCols ++ Map(
    "role_dw_id" -> "cx_user_role_dw_id",
  )

  val cxSchoolUserDeltaCols: Map[String, String] = cxSchoolUserCommonCols ++ Map(
    "role_id" -> "cx_user_role_id",
  )

  val cxSchoolUserCols: Map[String, String] = cxSchoolUserRedshiftCols ++ cxSchoolUserDeltaCols ++ Map("occurredOn" -> "occurredOn")

  val cxSchoolUserDtRedshiftCols: Seq[String] =  cxSchoolUserRedshiftCols.values.toSeq ++ cxUserCommonDatetimeCols
  val cxSchoolUserDtDeltaCols: Seq[String] =  cxSchoolUserDeltaCols.values.toSeq ++ cxUserCommonDatetimeCols

  // CX School Dimension
  val CxSchoolDimensionName = "cx-school-dimension"
  val CxSchoolParquetSource  = "parquet-cx-school-source"

  val CxSchoolStagingParquet = "parquet-stage-cx-school"
  val CxSchoolParquetSink  = "parquet-cx-school-sink"
  val CxSchoolRedshiftSink  = "redshift-cx-school-sink"
  val CxSchoolDeltaSink  = "delta-cx-school-sink"

  val CxSchoolEntityPrefix = "school_cx"
  val cxSchoolCols = Map(
    "uuid" -> "school_id",
    "id" -> "school_cx_id",
    "name_ar" -> "school_cx_name_ar",
    "zone" -> "school_cx_zone",
    "cluster" -> "school_cx_cluster",
    "batch" -> "school_cx_batch",
    "report" -> "school_cx_report",
    "status" -> "school_cx_status",
    "occurredOn" -> "school_cx_last_updated"
  )

  // CX Observation Indicator Facts
  val RedshiftObservationIndicatorSink = "redshift-cx-observation-indicator-sink"
  val RedshiftObservationSource = "redshift-cx-observation-source"
  val DeltaObservationIndicatorSink = "delta-cx-observation-indicator-sink"
  val ParquetObservationIndicatorSink = "parquet-cx-observation-indicator-sink"
  val ParquetObservationIndicatorSource = "parquet-cx-observation-indicator-source"
  val ParquetObservationIndicatorStagingSink = "parquet-cx-observation-indicator-staging-sink"
  val CxObservationIndicator = "cx-observation-indicator"

  val observationIndicatorEntity = "fcoi"
  val cxObservationIndicatorCols = Map(
    "id" -> "fcoi_id",
    "observation_id" -> "fcoi_observation_id",
    "indicator_id" -> "fcoi_indicator_id",
    "cx_observation_dw_id" -> "fcoi_observation_dw_id",
    "indicator_value" -> "fcoi_indicator_value",
    "coaching_time" -> "fcoi_coaching_time",
    "indicator_comment" -> "fcoi_indicator_comment",
    "coaching_comment" -> "fcoi_coaching_comment",
    "occurredOn" -> "occurredOn"
  )

  def getOperations(eventType: String): String = {
    val crudOperationsMap = Map(
      "AcademicYearStarted" -> Insert,
      "Create" -> Insert,
      "Update" -> Update,
      "Enable" -> Update,
      "Disable" -> Update,
      "Delete" -> Delete,
      "InstructionalPlanPublishedEvent" -> Insert,
      "InstructionalPlanRePublishedEvent" -> Insert,
      "LessonWorkflowStatusChangedEvent" -> Update,
      "LessonPublishedEvent" -> Update,
      "ContentPublishedEvent" -> Update,
      "Attached" -> InsertWithHistory,
      "Detached" -> InsertWithHistory,
      "ContentAssignedEvent" -> InsertWithHistory,
      "ContentUnAssignedEvent" -> InsertWithHistory,
      "LessonAssignedEvent" -> InsertWithHistory,
      "LessonUnAssignedEvent" -> InsertWithHistory
    )
    val operation = crudOperationsMap.filterKeys(k => eventType.toLowerCase.contains(k.toLowerCase()))
    operation.head._2
  }

}
