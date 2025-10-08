package com.alefeducation

import com.alefeducation.schema.Schema._
import com.alefeducation.schema.ccl.{ActivityPlannedInAbilityTestComponentSchema, InstructionalPlanInReviewEvent, PacingGuideCreatedEventSchema}
import com.alefeducation.service.{DataSink, SparkStreamingService}
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Resources._
import com.alefeducation.util.{DateField, SparkSessionUtils}
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class CCLEvents(override val session: SparkSession) extends SparkStreamingService {

  import CCLEvents._

  override val name: String = JobName

  val dateField: DateField = DateField("occurredOn", LongType)

  private lazy val kafkaStream = read(source, session)
  private lazy val payloadStream = getPayloadStream(session, kafkaStream).drop("tenantId")
  private lazy val payloadAndHeaderStream = getPayloadStream(session, kafkaStream, useKafkaHeader = true)

  def getDF(schema: StructType, events: String*): DataFrame = {
    process(session, payloadStream, schema, events.toList, dateField)
  }

  def getDfWithHeader(schema: StructType, events: String*): DataFrame = {
    process(session, payloadAndHeaderStream, schema, events.toList, dateField, includeHeaders = true)
  }

  override def transform(): List[DataSink] =
    List(
      DataSink("ccl-term-created-sink", getDF(termWeekSchema, TermWeekCreatedEvent)),
      DataSink("ccl-ip-published-sink", getDF(instructionalPlanPublishedSchema, InstructionalPlanPublishedEvent)),
      DataSink("ccl-ip-re-published-sink", getDF(instructionalPlanRePublishedSchema, InstructionalPlanRePublishedEvent)),
      DataSink("ccl-ip-deleted-sink", getDF(instructionalPlanDeletedSchema, InstructionalPlanDeletedEvent)),
      DataSink("ccl-lesson-mutated-sink", getDF(lessonMutatedSchema, LessonCreatedEvent, LessonMetadataUpdatedEvent)),
      DataSink("ccl-lesson-deleted-sink", getDF(lessonDeletedSchema, LessonDeletedEvent)),
      DataSink("ccl-lesson-published-sink", getDF(lessonPublishedSchema, LessonPublishedEvent)),
      DataSink("ccl-lesson-workflow-status-changed-sink", getDF(lessonWorkflowStatusChangedSchema, LessonWorkflowStatusChangedEvent)),
      DataSink("ccl-lesson-outcome-attach-sink", getDF(lessonOutcomeAttachSchema, LessonOutcomeAttachedEvent, LessonOutcomeDetachedEvent)),
      DataSink("ccl-lesson-content-attach-sink", getDF(lessonContentAttachSchema, LessonContentAttachedEvent, LessonContentDetachedEvent)),
      DataSink("ccl-lesson-skill-link-sink", getDF(lessonSkillLinkSchema, LessonSkillLinkedEvent, LessonSkillUnlinkedEvent)),
      DataSink("ccl-content-created-sink", getDF(contentCreatedSchema, ContentCreatedEvent)),
      DataSink("ccl-content-updated-sink", getDF(contentUpdatedSchema, ContentUpdatedEvent)),
      DataSink("ccl-content-deleted-sink", getDF(contentDeletedSchema, ContentDeletedEvent)),
      DataSink("ccl-content-published-sink", getDF(contentPublishedSchema, ContentPublishedEvent)),
      DataSink("ccl-content-outcome-attach-sink",
        getDF(contentOutcomeAttachSchema, ContentOutcomeAttachedEvent, ContentOutcomeDetachedEvent)),
      DataSink("ccl-content-skill-attached-detached-sink",
        getDF(contentSkillAttachedDetachedSchema, ContentSkillAttachedEvent, ContentSkillDetachedEvent)),
      DataSink("ccl-outcome-created-sink", getDF(outcomeCreatedSchema, OutcomeCreatedEvent)),
      DataSink("ccl-outcome-updated-sink", getDF(outcomeUpdatedSchema, OutcomeUpdatedEvent)),
      DataSink("ccl-outcome-deleted-sink", getDF(outcomeDeletedSchema, OutcomeDeletedEvent)),
      DataSink("ccl-curriculum-created-event-sink", getDF(CurriculumCreatedSchema, CurriculumCreatedEvent)),
      DataSink("ccl-curriculum-grade-created-event-sink", getDF(CurriculumGradeCreatedSchema, CurriculumGradeCreatedEvent)),
      DataSink("ccl-curriculum-subject-created-event-sink", getDF(CurriculumSubjectCreatedSchema, CurriculumSubjectCreatedEvent)),
      DataSink("ccl-curriculum-subject-updated-event-sink", getDF(CurriculumSubjectUpdatedSchema, CurriculumSubjectUpdatedEvent)),
      DataSink("ccl-curriculum-subject-deleted-event-sink", getDF(CurriculumSubjectDeletedSchema, CurriculumSubjectDeletedEvent)),
      DataSink("ccl-theme-created-sink", getDF(themeCreatedSchema, ThemeCreatedEvent)),
      DataSink("ccl-theme-updated-sink", getDF(themeUpdatedSchema, ThemeUpdatedEvent)),
      DataSink("ccl-theme-deleted-sink", getDF(themeDeletedSchema, ThemeDeletedEvent)),
      DataSink("ccl-lesson-template-created-sink", getDF(templateCreatedSchema, LessonTemplateCreatedEvent)),
      DataSink("ccl-lesson-template-updated-sink", getDF(templateUpdatedSchema, LessonTemplateUpdatedEvent)),
      DataSink("ccl-question-mutated-sink", getDfWithHeader(questionMutatedSchema, QuestionCreatedEvent, QuestionUpdatedEvent)),
      DataSink("ccl-question-pool-mutated-sink", getDF(questionPoolSchema, PoolCreatedEvent, PoolUpdatedEvent)),
      DataSink("ccl-question-pool-association-sink",
        getDF(questionPoolAssociationSchema, QuestionsAddedToPoolEvent, QuestionsRemovedFromPoolEvent)),
      DataSink("ccl-skill-mutated-sink", getDF(cclSkillMutatedSchema, SkillCreated, SkillUpdatedEvent)),
      DataSink("ccl-skill-deleted-sink", getDF(cclSkillDeletedSchema, SkillDeleted)),
      DataSink("ccl-skill-linked-unlinked-sink", getDF(cclSkillLinkToggleSchema, SkillLinkedEvent, SkillUnlinkedEvent)),
      DataSink("ccl-skill-linked-unlinked-v2-sink", getDF(cclSkillLinkToggleV2Schema, SkillLinkedEventV2, SkillUnlinkedEventV2)),
      DataSink("ccl-skill-category-link-unlink-sink",
        getDF(cclSkillCategoryLinkToggleSchema, SkillCategoryLinkedEvent, SkillCategoryUnlinkedEvent)),
      DataSink("ccl-outcome-skill-attach-sink", getDF(outcomeSkillLinkSchema, OutcomeSkillAttachedEvent, OutcomeSkillDetachedEvent)),
      DataSink("ccl-outcome-category-attach-sink",
        getDF(outcomeCategoryLinkSchema, OutcomeCategoryAttachedEvent, OutcomeCategoryDetachedEvent)),
      DataSink("ccl-category-created-sink", getDF(categoryCreatedSchema, CategoryCreatedEvent)),
      DataSink("ccl-category-updated-sink", getDF(categoryUpdatedSchema, CategoryUpdatedEvent)),
      DataSink("ccl-category-deleted-sink", getDF(categoryDeletedSchema, CategoryDeletedEvent)),
      DataSink("ccl-assignment-mutated-sink", getDF(assignmentMutatedSchema, AssignmentCreatedEvent, AssignmentUpdatedEvent)),
      DataSink("ccl-assignment-deleted-sink", getDF(assignmentDeletedSchema, AssignmentDeletedEvent)),
      DataSink("ccl-lesson-assignment-association-sink",
        getDF(lessonAssignmentAssociationSchema, LessonAssignmentAttachEvent, LessonAssignmentDetachedEvent)),
      DataSink(
        "ccl-lesson-assessment-rule-association-sink",
        getDF(lessonAssessmentRuleAssociationSchema,
          LessonAssessmentRuleAddedEvent,
          LessonAssessmentRuleUpdatedEvent,
          LessonAssessmentRuleRemovedEvent)
      ),
      DataSink(
        "ccl-guardian-sink",
        getDF(guardianInvitedRegisteredEventSchema, AdminGuardianRegistered)),
      DataSink("ccl-guardian-deleted-sink",
        getDF(guardiansDeletedEventSchema, AdminGuardianDeleted)),
      DataSink("ccl-guardian-association-sink",
        getDF(guardianAssociationSchema, AdminGuardianAssociations)),

      DataSink("ccl-activity-template-sink", getDF(activityTemplateSchema, ActivityTemplatePublishedEvent)),
      DataSink("ccl-activity-outcome-sink", getDF(activityOutcomeSchema, ActivityOutcomeAttachedEvent, ActivityOutcomeDetachedEvent)),
      DataSink("ccl-activity-metadata-sink", getDF(activityMetadataSchema, ActivityMetadataCreatedEvent, ActivityMetadataUpdatedEvent, ActivityPublishedEvent)),
      DataSink("ccl-activity-metadata-delete-sink", getDF(activityMetadataDeletedSchema, ActivityMetadataDeletedEvent)),
      DataSink("ccl-activity-workflow-status-changed-sink", getDF(activityWorkflowStatusChangedSchema, ActivityWorkflowStatusChangedEvent)),
      DataSink("ccl-organization-mutated-sink", getDF(organizationSchema, OrganizationCreatedEvent, OrganizationUpdatedEvent)),
      DataSink("ccl-content-repository-created-sink", getDfWithHeader(contentRepositoryCreatedSchema, ContentRepositoryCreatedEvent)),
      DataSink("ccl-content-repository-updated-sink", getDfWithHeader(contentRepositoryCreatedSchema, ContentRepositoryUpdatedEvent)),
      DataSink("ccl-content-repository-deleted-sink", getDfWithHeader(contentRepositoryDeletedSchema, ContentRepositoryDeletedEvent)),
      DataSink("ccl-content-repository-material-attached-sink", getDfWithHeader(contentRepositoryAttachedSchema, ContentRepositoryMaterialAttachedEvent)),
      DataSink("ccl-content-repository-material-detached-sink", getDfWithHeader(contentRepositoryDetachedSchema, ContentRepositoryMaterialDetachedEvent)),

      DataSink("ccl-pathway-draft-created-sink", getDfWithHeader(pathwayDraftCreatedSchema, DraftPathwayCreatedEvent)),
      DataSink("ccl-pathway-in-review-sink", getDfWithHeader(pathwayInReviewSchema, InReviewPathwayEvent)),
      DataSink("ccl-pathway-published-sink", getDfWithHeader(pathwayPublishedSchema, PathwayPublishedEvent)),
      DataSink("ccl-pathway-details-updated-sink", getDfWithHeader(pathwayDetailsUpdatedSchema, PathwayDetailsUpdatedEvent)),
      DataSink("ccl-pathway-deleted-sink", getDfWithHeader(pathwayDeletedSchema, PathwayDeletedEvent)),
      DataSink("ccl-level-published-with-pathway-sink", getDfWithHeader(levelPublishedWithPathwaySchema, LevelPublishedWithPathwayEvent)),
      DataSink("ccl-level-activity-published-with-pathway-sink", getDfWithHeader(levelPublishedWithPathwaySchema, LevelPublishedWithPathwayEvent)),

      DataSink("ccl-published-activity-planned-in-course-sink", filterPublished(getDfWithHeader(activityPlannedInCourseSchema, ActivityPlannedInCourseEvent))),
      DataSink("ccl-published-activity-unplanned-in-course-sink", filterPublished(getDfWithHeader(activityUnPlannedInCourseSchema, ActivityUnPlannedInCourseEvent))),
      DataSink("ccl-published-activity-updated-in-course-sink", filterPublished(getDfWithHeader(activityUpdatedInCourseSchema, ActivityUpdatedInCourseEvent))),

      DataSink("ccl-published-interim-checkpoint-created-sink", filterPublished(getDfWithHeader(interimCheckpointPublishedSchema, InterimCheckpointPublishedEvent))),
      DataSink("ccl-published-interim-checkpoint-updated-sink", filterPublished(getDfWithHeader(interimCheckpointUpdatedSchema, InterimCheckpointUpdatedEvent))),
      DataSink("ccl-published-interim-checkpoint-deleted-sink", filterPublished(getDfWithHeader(interimCheckpointDeletedSchema, InterimCheckpointDeletedEvent))),

      DataSink("ccl-in-review-activity-planned-in-course-sink", filterInReview(getDfWithHeader(activityPlannedInCourseSchema, ActivityPlannedInCourseEvent))),
      DataSink("ccl-in-review-activity-unplanned-in-course-sink", filterInReview(getDfWithHeader(activityUnPlannedInCourseSchema, ActivityUnPlannedInCourseEvent))),
      DataSink("ccl-in-review-activity-updated-in-course-sink", filterInReview(getDfWithHeader(activityPlannedInCourseSchema, ActivityUpdatedInCourseEvent))),

      DataSink("ccl-in-review-interim-checkpoint-created-sink", filterInReview(getDfWithHeader(interimCheckpointPublishedSchema, InterimCheckpointPublishedEvent))),
      DataSink("ccl-in-review-interim-checkpoint-updated-sink", filterInReview(getDfWithHeader(interimCheckpointUpdatedSchema, InterimCheckpointUpdatedEvent))),
      DataSink("ccl-in-review-interim-checkpoint-deleted-sink", filterInReview(getDfWithHeader(interimCheckpointDeletedSchema, InterimCheckpointDeletedEvent))),

      DataSink("ccl-published-level-added-in-pathway-sink", filterPublished(getDfWithHeader(levelAddedInPathwaySchema, LevelAddedInPathwayEvent))),
      DataSink("ccl-published-level-updated-in-pathway-sink", filterPublished(getDfWithHeader(levelUpdatedInPathwaySchema, LevelUpdatedInPathwayEvent))),
      DataSink("ccl-published-level-deleted-in-pathway-sink", filterPublished(getDfWithHeader(levelDeletedFromPathwaySchema, LevelDeletedFromPathwayEvent))),

      DataSink("ccl-in-review-level-added-in-pathway-sink", filterInReview(getDfWithHeader(levelAddedInPathwaySchema, LevelAddedInPathwayEvent))),
      DataSink("ccl-in-review-level-updated-in-pathway-sink", filterInReview(getDfWithHeader(levelUpdatedInPathwaySchema, LevelUpdatedInPathwayEvent))),
      DataSink("ccl-in-review-level-deleted-in-pathway-sink", filterInReview(getDfWithHeader(levelDeletedFromPathwaySchema, LevelDeletedFromPathwayEvent))),

      DataSink("ccl-published-adt-planned-in-course-sink", filterPublished(getDfWithHeader(adtPlannedInCourseSchema, ADTPlannedInCourseEvent))),
      DataSink("ccl-published-adt-unplanned-in-course-sink", filterPublished(getDfWithHeader(adtUnPlannedInCourseSchema, ADTUnPlannedInCourseEvent))),
      DataSink("ccl-published-adt-updated-in-course-sink", filterPublished(getDfWithHeader(adtUpdatedCourseSchema, ADTUnPlannedInCourseEvent))),

      DataSink("ccl-in-review-adt-planned-in-course-sink", filterInReview(getDfWithHeader(adtPlannedInCourseSchema, ADTPlannedInCourseEvent))),
      DataSink("ccl-in-review-adt-unplanned-in-course-sink", filterInReview(getDfWithHeader(adtUnPlannedInCourseSchema, ADTUnPlannedInCourseEvent))),

      DataSink("ccl-marketplace-avatar-created-sink", getDfWithHeader(avatarCreatedEventSchema, AvatarCreatedEvent)),
      DataSink("ccl-marketplace-avatar-updated-sink", getDfWithHeader(avatarUpdatedEventSchema, AvatarUpdatedEvent)),
      DataSink("ccl-marketplace-avatar-deleted-sink", getDfWithHeader(avatarDeletedEventSchema, AvatarDeletedEvent)),
      DataSink("ccl-marketplace-avatar-layer-created-sink", getDfWithHeader(avatarLayerCreatedEventSchema, AvatarLayerCreatedEvent)),
    )
}

object CCLEvents {

  val JobName = "ccl-events"

  val source = "ccl-source"

  def main(args: Array[String]): Unit = {
    new CCLEvents(SparkSessionUtils.getSession(JobName)).run
  }

}
