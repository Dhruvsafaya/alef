package com.alefeducation

import com.alefeducation.schema.Schema._
import com.alefeducation.schema.ccl.PacingGuideDeletedEventSchema
import com.alefeducation.service.{DataSink, SparkStreamingService}
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility.process
import com.alefeducation.util.Resources.getPayloadStream
import com.alefeducation.util.{DateField, SparkSessionUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, TimestampType}

class AllRawEvents(override val session: SparkSession) extends SparkStreamingService {

  override val name: String = AllRawEvents.name

  override def transform(): List[DataSink] = {
    val kafkaStream = read("raw-events-source", session)
    val payloadStream = getPayloadStream(session, kafkaStream)
    val dateCreatedOnEpoch = DateField("createdOn", LongType)
    val dateCreatedOnTs = DateField("createdOn", TimestampType)
    val dateOccurredOnTs = DateField("occurredOn", TimestampType)
    val dateOccurredOnEpoch = DateField("occurredOn", LongType)

    val sinks = List(
      DataSink(
        "admin-school-sink",
        process(session, payloadStream, schoolSchema, List(AdminSchoolCreated, AdminSchoolUpdated, AdminSchoolDeleted), dateCreatedOnEpoch)),
      DataSink("admin-grade-sink",
               process(session, payloadStream, gradeSchema, List(AdminGradeCreated, AdminGradeUpdated), dateCreatedOnEpoch)),
      DataSink("admin-grade-deleted-sink",
               process(session, payloadStream, gradeDeletedSchema, List(AdminGradeDeleted), dateCreatedOnEpoch)),
      DataSink(
        "admin-subject-sink",
        process(session,
                payloadStream,
                subjectSchema,
                List(AdminSubjectCreated, AdminSubjectUpdated, AdminSubjectDeleted),
                dateCreatedOnEpoch)
      ),
      DataSink("admin-section-sink",
               process(session, payloadStream, sectionSchema, List(AdminSectionCreated, AdminSectionUpdated), dateCreatedOnEpoch)),
      DataSink(
        "admin-section-state-changed-sink",
        process(session,
                payloadStream,
                sectionStateChangedSchema,
                List(AdminSectionEnabled, AdminSectionDisabled, AdminSectionDeleted),
                dateCreatedOnEpoch)
      ),
      DataSink(
        "admin-teacher-sink",
        process(session,
                payloadStream,
                teacherSchema,
                List(AdminTeacher, AdminTeacherUpdate, AdminTeacherEnabledEvent, AdminTeacherDisabledEvent),
                dateCreatedOnEpoch)
      ),
      DataSink("admin-student-sink",
               process(session, payloadStream, studentSchema, List(AdminStudent, AdminStudentUpdated), dateCreatedOnEpoch)),
      DataSink("admin-student-section-updated-sink",
               process(session, payloadStream, studentSectionUpdatedSchema, List(AdminStudentSectionUpdated), dateOccurredOnEpoch)),
      DataSink(
        "admin-student-status-toggle-sink",
        process(session,
                payloadStream,
                studentStatusToggleSchema,
                List(AdminStudentEnabledEvent, AdminStudentDisabledEvent),
                dateCreatedOnEpoch)
      ),
      DataSink(
        "admin-student-school-grade-move-sink",
        process(session,
                payloadStream,
                studentSchoolGradeMoveSchema,
                List(AdminStudentGradeMovementEvent, AdminStudentMovedBetweenSchools),
                dateOccurredOnEpoch)
      ),
      DataSink("admin-student-promoted-sink",
               process(session, payloadStream, studentPromotedSchema, List(AdminStudentPromotedEvent), dateOccurredOnEpoch)),
      DataSink("admin-student-tag-updated-sink",
               process(session, payloadStream, studentTagUpdatedSchema, List(StudentTagUpdatedEvent), dateOccurredOnEpoch)),
      DataSink("admin-students-deleted-sink",
               process(session, payloadStream, studentsDeletedSchema, List(AdminStudentsDeleted), dateCreatedOnEpoch)),
      DataSink(
        "admin-learning-path-sink",
        process(session,
                payloadStream,
                learningPathSchema,
                List(AdminLearningPath, AdminLearningPathUpdated, AdminLearningPathDeleted),
                dateCreatedOnEpoch)
      ),
      DataSink(
        "admin-academic-year-started-sink",
        process(session,
                payloadStream,
                academicYearMutated,
                List(AcademicYearStarted),
                dateOccurredOnEpoch)
      ),
      DataSink(
        "admin-academic-year-date-updated-sink",
        process(session,
                payloadStream,
                academicYearMutated,
                List(AcademicYearUpdated),
                dateOccurredOnEpoch)
      ),
      DataSink(
        "admin-academic-year-roll-over-completed-sink",
        process(session,
                payloadStream,
                academicYearRollOverSchema,
                List(AcademicYearRollOverCompleted),
                dateOccurredOnEpoch)
      ),
      DataSink(
        "admin-school-academic-year-roll-over-completed-sink",
        process(session,
                payloadStream,
                academicYearRollOverSchema,
                List(AcademicYearRollOverCompleted),
                dateOccurredOnEpoch)
      ),
      DataSink(
        "admin-school-academic-year-switched-sink",
        process(session, payloadStream, schoolAcademicYearSwitchedSchema, List(SchoolAcademicYearSwitched), dateOccurredOnEpoch)
      ),
      DataSink("learning-objective-sink",
               process(session, payloadStream, learningObjectiveSchema, List(LearningObjectiveCreated), dateCreatedOnEpoch)),
      DataSink(
        "learning-objective-updated-deleted-sink",
        process(session,
                payloadStream,
                learningObjectiveUpdatedDeletedSchema,
                List(LearningObjectiveUpdated, LearningObjectiveDeleted),
                dateOccurredOnEpoch)
      ),
      DataSink("conversation-occurred-sink",
               process(session, payloadStream, conversationOccurredSchema, List(ConversationOccurred), dateCreatedOnEpoch)),
      DataSink("authentication-sink", process(session, payloadStream, userLoggedIn, List(UserLogin), dateCreatedOnTs)),
      DataSink("award-resource-sink", process(session, payloadStream, awardResourceSchema, List(AwardResource), dateCreatedOnTs)),
      DataSink("user-sink",
               process(session, payloadStream, userSchema, List(UserCreated, UserUpdated, UserEnabled, UserDisabled), dateOccurredOnEpoch)),
      DataSink("role-sink",
        process(session, payloadStream, roleSchema, List(RoleCreatedEvent, RoleUpdatedEvent, RoleDeletedEvent), dateOccurredOnEpoch)),
      DataSink("teacher-school-moved-sink",
               process(session, payloadStream, teacherSchoolMovedSchema, List(TeacherSchoolMoved), dateOccurredOnEpoch)),
      DataSink("no-skill-content-sink",
               process(session, payloadStream, noMloForSkillsFoundSchema, List(NoMloForSkillsFound), dateOccurredOnEpoch)),
      DataSink("kt-game-session-created-sink",
               process(session, payloadStream, ktGameSessionCreatedSchema, List(KTGameSessionCreated), dateOccurredOnTs)),
      DataSink("kt-game-session-started-sink",
               process(session, payloadStream, ktGameSessionStartedSchema, List(KTGameSessionStarted), dateOccurredOnTs)),
      DataSink("kt-game-session-finished-sink",
               process(session, payloadStream, ktGameSessionFinishedSchema, List(KTGameSessionFinished), dateOccurredOnTs)),
      DataSink(
        "kt-game-question-session-started-sink",
        process(session, payloadStream, ktGameQuestionSessionStartedSchema, List(KTGameQuestionSessionStarted), dateOccurredOnTs)
      ),
      DataSink(
        "kt-game-question-session-finished-sink",
        process(session, payloadStream, ktGameQuestionSessionFinishedSchema, List(KTGameQuestionSessionFinished), dateOccurredOnTs)
      ),
      DataSink(
        "kt-game-session-creation-skipped-sink",
        process(session, payloadStream, ktGameSessionCreationSkippedSchema, List(KTGameSessionCreationSkipped), dateOccurredOnTs)
      ),
      DataSink("in-class-game-sink",
               process(session, payloadStream, incGameSchema, List(IncGameCreated, IncGameUpdated), dateOccurredOnTs)),
      DataSink(
        "in-class-game-session-sink",
        process(
          session,
          payloadStream,
          incGameSessionSchema,
          List(IncGameSessionStarted, IncGameSessionFinished, IncGameSessionCancelled, IncGameSessionCreated),
          dateOccurredOnTs
        )
      ),
      DataSink("in-class-game-outcome-sink", process(session, payloadStream, incGameOutcomeSchema, List(IncGameOutcome), dateOccurredOnTs)),
      DataSink(
        "lesson-feedback-sink",
        process(session, payloadStream, lessonFeedback, List(LessonFeedbackSubmitted, LessonFeedbackCancelled), dateOccurredOnEpoch)),
      DataSink(
        "assignment-mutated-sink",
        process(session, payloadStream, assignmentMutatedSchema, List(AssignmentCreatedEvent, AssignmentUpdatedEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "assignment-instance-mutated-sink",
        process(session,
                payloadStream,
                assignmentInstanceMutatedSchema,
                List(AssignmentInstanceCreatedEvent, AssignmentInstanceUpdatedEvent),
                dateOccurredOnEpoch)
      ),
      DataSink(
        "assignment-instance-students-updated-sink",
        process(session,
                payloadStream,
                assignmentInstanceStudentsUpdatedSchema,
                List(AssignmentInstanceStudentsUpdatedEvent),
                dateOccurredOnEpoch)
      ),
      DataSink(
        "assignment-resubmission-requested-sink",
        process(session,
          payloadStream,
          AssignmentResubmissionRequestedEventSchema,
          List(AssignmentResubmissionRequestedEvent),
          dateOccurredOnEpoch)
      ),
      DataSink("assignment-deleted-sink",
               process(session, payloadStream, assignmentDeletedSchema, List(AssignmentDeletedEvent), dateOccurredOnEpoch)),
      DataSink(
        "assignment-instance-deleted-sink",
        process(session, payloadStream, assignmentInstanceDeletedSchema, List(AssignmentInstanceDeletedEvent), dateOccurredOnEpoch)
      ),
      DataSink("assignment-submission-sink",
               process(session, payloadStream, assignmentSubmittedSchema, List(AssignmentSubmissionMutatedEvent), dateOccurredOnEpoch)),
      DataSink("class-modified-sink",
               process(session, payloadStream, classModifiedSchema, List(ClassCreatedEvent, ClassUpdatedEvent), dateOccurredOnEpoch)),
      DataSink("class-deleted-sink", process(session, payloadStream, classDeletedSchema, List(ClassDeletedEvent), dateOccurredOnEpoch)),
      DataSink(
        "student-class-association-sink",
        process(session,
                payloadStream,
                studentClassAssociationSchema,
                List(StudentEnrolledInClassEvent, StudentUnenrolledFromClassEvent),
                dateOccurredOnEpoch)
      ),
      DataSink("admin-tag-sink", process(session, payloadStream, tagEventSchema, List(TaggedEvent, UntaggedEvent), dateOccurredOnEpoch)),
      DataSink(
        "class-schedule-slot-sink",
        process(session,
                payloadStream,
                classScheduleSlotSchema,
                List(ClassScheduleSlotAddedEvent, ClassScheduleSlotDeletedEvent),
                dateOccurredOnEpoch)
      ),
      DataSink(
        "user-moved-sink",
        process(session, payloadStream, userMovedSchema, List(UserMoved), dateOccurredOnEpoch)
      ),
      DataSink(
        "school-status-toggle-sink",
        process(session, payloadStream, schoolStatusToggleSchema, List(SchoolActivated, SchoolDeactivated), dateOccurredOnEpoch)
      ),
      DataSink("class-category-sink",
        process(session, payloadStream, classCategorySchema, List(ClassCategoryCreatedEvent,ClassCategoryUpdatedEvent,ClassCategoryDeletedEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "user-deleted-sink",
        process(session, payloadStream, userDeletedSchema, List(UserDeletedEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "badge-updated-sink",
        process(session, payloadStream, badgeUpdatedSchema, List(BadgesMetaDataUpdatedEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "badge-awarded-sink",
        process(session, payloadStream, studentBadgeAwardedSchema, List(StudentBadgeAwardedEvent), dateOccurredOnEpoch)
      ),
      DataSink("leaderboard-events-sink",
        process(session, payloadStream, leaderboardEventSchema, List(PathwayLeaderboardUpdatedEvent), dateOccurredOnEpoch)
      ),
      DataSink("certificate-events-sink",
        process(session, payloadStream, certificateAwardedEventSchema, List(CertificateAwardedEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "user-avatar-sink",
        process(session, payloadStream, userAvatarSchema, List(UserAvatarSelectedEvent, UserAvatarUpdatedEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "pathway-class-target-sink",
        process(session, payloadStream, pathwayTargetMutatedEventSchema, List(PathwayTargetMutatedEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "pathway-student-target-sink",
        process(session, payloadStream, studentPathwayTargetMutatedSchema, List(StudentPathwayTargetMutatedEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "reward-purchase-transaction-sink",
        process(session, payloadStream, purchaseTransactionEventSchema, List(PurchaseTransactionEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "marketplace-purchase-sink",
        process(session, payloadStream, itemPurchasedEventSchema, List(ItemPurchasedEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "marketplace-avatar-created-sink",
        process(session, payloadStream, avatarCreatedEventSchema, List(AvatarCreatedEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "marketplace-avatar-updated-sink",
        process(session, payloadStream, avatarUpdatedEventSchema, List(AvatarUpdatedEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "marketplace-avatar-deleted-sink",
        process(session, payloadStream, avatarDeletedEventSchema, List(AvatarDeletedEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "activity-settings-open-path-sink",
        process(session, payloadStream, activitySettingsOpenPathEventSchema, List(ActivitySettingsOpenPathEventEnabled, ActivitySettingsOpenPathEventDisabled), dateOccurredOnTs)
      ),
      DataSink(
        "activity-settings-component-visibility-sink",
        process(session, payloadStream, activitySettingsComponentVisibilityEventSchema, List(ActivitySettingsComponentVisibilityEventHide, ActivitySettingsComponentVisibilityEventShow), dateOccurredOnTs)
      ),
      DataSink(
        "authoring-course-pacing-guide-created-sink",
        process(session, payloadStream, pacingGuideCreatedEventSchema, List(PacingGuideCreatedEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "authoring-course-pacing-guide-updated-sink",
        process(session, payloadStream, pacingGuideUpdatedEventSchema, List(PacingGuideUpdatedEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "authoring-course-pacing-guide-deleted-sink",
        process(session, payloadStream, pacingGuideDeletedEventSchema, List(PacingGuideDeletedEvent), dateOccurredOnEpoch)
      ),
      DataSink("academic-year-created-sink",
        process(session, payloadStream, academicYearMutatedEventSchema, List(AcademicYearCreatedEvent), dateOccurredOnEpoch)
      ),
      DataSink("academic-year-updated-sink",
        process(session, payloadStream, academicYearMutatedEventSchema, List(AcademicYearUpdatedEvent), dateOccurredOnEpoch)
      ),
      DataSink("academic-calendar-created-sink",
        process(session, payloadStream, academicCalendarMutatedEventSchema, List(AcademicCalendarCreatedEvent), dateOccurredOnEpoch)
      ),
      DataSink("academic-calendar-updated-sink",
        process(session, payloadStream, academicCalendarMutatedEventSchema, List(AcademicCalendarUpdatedEvent), dateOccurredOnEpoch)
      ),
      DataSink("academic-calendar-deleted-sink",
        process(session, payloadStream, academicCalendarMutatedEventSchema, List(AcademicCalendarDeletedEvent), dateOccurredOnEpoch)
      ),
      DataSink("staff-user-sink",
        process(session, payloadStream, userSchema, List(UserCreated, UserUpdated, UserEnabled, UserDisabled), dateOccurredOnEpoch)
      ),
      DataSink(
        "staff-user-moved-sink",
        process(session, payloadStream, userMovedSchema, List(UserMoved), dateOccurredOnEpoch)
      ),
      DataSink(
        "staff-user-deleted-sink",
        process(session, payloadStream, userDeletedSchema, List(UserDeletedEvent), dateOccurredOnEpoch)
      ),
      DataSink(
        "challenge-game-progress-event-sink",
        process(session, payloadStream, challengeGameProgressEventSchema, List(AlefGameChallengeProgressEvent), dateOccurredOnEpoch)
      )
    )

    sinks
  }
}

object AllRawEvents {

  private val name = "all-events"

  def main(args: Array[String]): Unit = {
    new AllRawEvents(SparkSessionUtils.getSession(name)).run
  }
}
