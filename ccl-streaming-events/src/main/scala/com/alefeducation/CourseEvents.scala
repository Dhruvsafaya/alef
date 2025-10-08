package com.alefeducation

import com.alefeducation.bigdata.Sink
import com.alefeducation.schema.Schema.{abilityTestComponentDisabledSchema, abilityTestComponentEnabledSchema, abilityTestComponentUpdatedSchema, activityPlannedInAbilityTestComponentSchema, activityPlannedInCourseSchema, activityUnPlannedInAbilityTestComponentSchema, activityUnPlannedInCourseSchema, activityUpdatedInCourseSchema, additionalResourceActivityPlannedSchema, additionalResourceActivityUnplannedSchema, additionalResourceActivityUpdatedSchema, containerAddedInCourseSchema, containerDeletedFromCourseSchema, containerPublishedWithCourseSchema, containerUpdatedInCourseSchema, courseDeletedSchema, courseDraftCreatedSchema, courseInReviewSchema, courseInstructionalPlanDeletedEventSchema, courseInstructionalPlanInReviewEventSchema, courseInstructionalPlanPublishedEventSchema, coursePublishedSchema, courseSettingsUpdatedSchema, downloadableResourcePlannedSchema, downloadableResourceUnplannedSchema, downloadableResourceUpdatedSchema, interimCheckpointDeletedSchema, interimCheckpointPublishedSchema, interimCheckpointUpdatedSchema, testPlannedInAbilityTestComponentSchema, testUnPlannedInAbilityTestComponentSchema, testUpdatedInAbilityTestComponentSchema}
import com.alefeducation.service.{DataSink, SparkStreamingService}
import com.alefeducation.util.Constants.{AbilityTestComponentDisabledEvent, AbilityTestComponentEnabledEvent, AbilityTestComponentUpdatedEvent, ActivityPlannedInAbilityTestComponentEvent, ActivityPlannedInCourseEvent, ActivityUnPlannedInAbilityTestComponentEvent, ActivityUnPlannedInCourseEvent, ActivityUpdatedInCourseEvent, AdditionalResourceActivityPlannedEvent, AdditionalResourceActivityUnplannedEvent, AdditionalResourceActivityUpdatedEvent, ContainerAddedInCourseEvent, ContainerDeletedFromCourseEvent, ContainerPublishedWithCourseEvent, ContainerUpdatedInCourseEvent, CourseDeletedEvent, CourseInstructionalPlanDeletedEvent, CourseInstructionalPlanInReviewEvent, CourseInstructionalPlanPublishedEvent, CoursePublishedEvent, CourseSettingsUpdatedEvent, DownloadableResourcePlannedEvent, DownloadableResourceUnplannedEvent, DownloadableResourceUpdatedEvent, DraftCourseCreatedEvent, InReviewCourseEvent, InterimCheckpointDeletedEvent, InterimCheckpointPublishedEvent, InterimCheckpointUpdatedEvent, TestPlannedInAbilityTestComponentEvent, TestUnPlannedInAbilityTestComponentEvent, TestUpdatedInAbilityTestComponentEvent}
import com.alefeducation.util.DataFrameUtility.{filterInReview, filterPublished, process}
import com.alefeducation.util.Resources.getPayloadStream
import com.alefeducation.util.{DateField, SparkSessionUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{LongType, StructType}

class CourseEvents(override val session: SparkSession) extends SparkStreamingService {
  import CourseEvents._
  override val name: String = JobName

  val dateField: DateField = DateField("occurredOn", LongType)

  private lazy val kafkaStream = read("ccl-course-source", session)
  private lazy val payloadAndHeaderStream = getPayloadStream(session, kafkaStream, useKafkaHeader = true)

  def getDfWithHeader(schema: StructType, events: String*): DataFrame = {
    process(session, payloadAndHeaderStream, schema, events.toList, dateField, includeHeaders = true)
  }

  override def transform(): List[Sink] = {
    List(
      DataSink("ccl-course-draft-created-sink", getDfWithHeader(courseDraftCreatedSchema, DraftCourseCreatedEvent)),
      DataSink("ccl-course-in-review-sink", getDfWithHeader(courseInReviewSchema, InReviewCourseEvent)),
      DataSink("ccl-course-published-sink", getDfWithHeader(coursePublishedSchema, CoursePublishedEvent)),
      DataSink("ccl-course-settings-updated-sink", getDfWithHeader(courseSettingsUpdatedSchema, CourseSettingsUpdatedEvent)),
      DataSink("ccl-course-deleted-sink", getDfWithHeader(courseDeletedSchema, CourseDeletedEvent)),
      DataSink("ccl-container-published-with-course-sink", getDfWithHeader(containerPublishedWithCourseSchema, ContainerPublishedWithCourseEvent)),
      DataSink("ccl-container-activity-published-with-course-sink", getDfWithHeader(containerPublishedWithCourseSchema, ContainerPublishedWithCourseEvent)),

      DataSink("ccl-published-container-added-in-course-sink", filterPublished(getDfWithHeader(containerAddedInCourseSchema, ContainerAddedInCourseEvent))),
      DataSink("ccl-published-container-updated-in-course-sink", filterPublished(getDfWithHeader(containerUpdatedInCourseSchema, ContainerUpdatedInCourseEvent))),
      DataSink("ccl-published-container-deleted-in-course-sink", filterPublished(getDfWithHeader(containerDeletedFromCourseSchema, ContainerDeletedFromCourseEvent))),

      DataSink("ccl-published-ability-test-enabled-in-course-sink", filterPublished(getDfWithHeader(abilityTestComponentEnabledSchema, AbilityTestComponentEnabledEvent))),
      DataSink("ccl-published-ability-test-disabled-in-course-sink", filterPublished(getDfWithHeader(abilityTestComponentDisabledSchema, AbilityTestComponentDisabledEvent))),
      DataSink("ccl-published-activity-planned-in-ability-test-component-in-course-sink", filterPublished(getDfWithHeader(activityPlannedInAbilityTestComponentSchema, ActivityPlannedInAbilityTestComponentEvent))),
      DataSink("ccl-published-activity-unplanned-in-ability-test-component-in-course-sink", filterPublished(getDfWithHeader(activityUnPlannedInAbilityTestComponentSchema, ActivityUnPlannedInAbilityTestComponentEvent))),
      DataSink("ccl-published-activity-updated-in-ability-test-component-in-course-sink", filterPublished(getDfWithHeader(abilityTestComponentUpdatedSchema, AbilityTestComponentUpdatedEvent))),

      DataSink("ccl-published-test-planned-in-ability-test-component-in-course-sink", filterPublished(getDfWithHeader(testPlannedInAbilityTestComponentSchema, TestPlannedInAbilityTestComponentEvent))),
      DataSink("ccl-published-test-unplanned-in-ability-test-component-in-course-sink", filterPublished(getDfWithHeader(testUnPlannedInAbilityTestComponentSchema, TestUnPlannedInAbilityTestComponentEvent))),
      DataSink("ccl-published-test-updated-in-ability-test-component-in-course-sink", filterPublished(getDfWithHeader(testUpdatedInAbilityTestComponentSchema, TestUpdatedInAbilityTestComponentEvent))),
      DataSink("ccl-in-review-test-planned-in-ability-test-component-in-course-sink", filterInReview(getDfWithHeader(testPlannedInAbilityTestComponentSchema, TestPlannedInAbilityTestComponentEvent))),
      DataSink("ccl-in-review-test-unplanned-in-ability-test-component-in-course-sink", filterInReview(getDfWithHeader(testUnPlannedInAbilityTestComponentSchema, TestUnPlannedInAbilityTestComponentEvent))),
      DataSink("ccl-in-review-test-updated-in-ability-test-component-in-course-sink", filterInReview(getDfWithHeader(testUpdatedInAbilityTestComponentSchema, TestUpdatedInAbilityTestComponentEvent))),

      DataSink("ccl-published-course-instructional-plan-published-sink", filterPublished(getDfWithHeader(courseInstructionalPlanPublishedEventSchema, CourseInstructionalPlanPublishedEvent))),
      DataSink("ccl-published-course-instructional-plan-in-review-sink", filterPublished(getDfWithHeader(courseInstructionalPlanInReviewEventSchema, CourseInstructionalPlanInReviewEvent))),
      DataSink("ccl-published-course-instructional-plan-deleted-sink", filterPublished(getDfWithHeader(courseInstructionalPlanDeletedEventSchema, CourseInstructionalPlanDeletedEvent))),

      DataSink("ccl-published-activity-planned-in-courses-sink", filterPublished(getDfWithHeader(activityPlannedInCourseSchema, ActivityPlannedInCourseEvent))),
      DataSink("ccl-published-activity-unplanned-in-courses-sink", filterPublished(getDfWithHeader(activityUnPlannedInCourseSchema, ActivityUnPlannedInCourseEvent))),
      DataSink("ccl-published-activity-updated-in-courses-sink", filterPublished(getDfWithHeader(activityUpdatedInCourseSchema, ActivityUpdatedInCourseEvent))),

      DataSink("ccl-published-interim-checkpoint-in-courses-created-sink", filterPublished(getDfWithHeader(interimCheckpointPublishedSchema, InterimCheckpointPublishedEvent))),
      DataSink("ccl-published-interim-checkpoint-in-courses-updated-sink", filterPublished(getDfWithHeader(interimCheckpointUpdatedSchema, InterimCheckpointUpdatedEvent))),
      DataSink("ccl-published-interim-checkpoint-in-courses-deleted-sink", filterPublished(getDfWithHeader(interimCheckpointDeletedSchema, InterimCheckpointDeletedEvent))),

      DataSink("ccl-in-review-activity-planned-in-courses-sink", filterInReview(getDfWithHeader(activityPlannedInCourseSchema, ActivityPlannedInCourseEvent))),
      DataSink("ccl-in-review-activity-unplanned-in-courses-sink", filterInReview(getDfWithHeader(activityUnPlannedInCourseSchema, ActivityUnPlannedInCourseEvent))),
      DataSink("ccl-in-review-activity-updated-in-courses-sink", filterInReview(getDfWithHeader(activityPlannedInCourseSchema, ActivityUpdatedInCourseEvent))),

      DataSink("ccl-in-review-interim-checkpoint-in-courses-created-sink", filterInReview(getDfWithHeader(interimCheckpointPublishedSchema, InterimCheckpointPublishedEvent))),
      DataSink("ccl-in-review-interim-checkpoint-in-courses-updated-sink", filterInReview(getDfWithHeader(interimCheckpointUpdatedSchema, InterimCheckpointUpdatedEvent))),
      DataSink("ccl-in-review-interim-checkpoint-in-courses-deleted-sink", filterInReview(getDfWithHeader(interimCheckpointDeletedSchema, InterimCheckpointDeletedEvent))),

      DataSink("ccl-in-review-additional-resource-activity-planned-in-courses-sink", filterInReview(getDfWithHeader(additionalResourceActivityPlannedSchema, AdditionalResourceActivityPlannedEvent))),
      DataSink("ccl-in-review-additional-resource-activity-unplanned-in-courses-sink", filterInReview(getDfWithHeader(additionalResourceActivityUnplannedSchema, AdditionalResourceActivityUnplannedEvent))),
      DataSink("ccl-in-review-additional-resource-activity-updated-in-courses-sink", filterInReview(getDfWithHeader(additionalResourceActivityUpdatedSchema, AdditionalResourceActivityUpdatedEvent))),
      DataSink("ccl-published-additional-resource-activity-planned-in-courses-sink", filterPublished(getDfWithHeader(additionalResourceActivityPlannedSchema, AdditionalResourceActivityPlannedEvent))),
      DataSink("ccl-published-additional-resource-activity-unplanned-in-courses-sink", filterPublished(getDfWithHeader(additionalResourceActivityUnplannedSchema, AdditionalResourceActivityUnplannedEvent))),
      DataSink("ccl-published-additional-resource-activity-updated-in-courses-sink", filterPublished(getDfWithHeader(additionalResourceActivityUpdatedSchema, AdditionalResourceActivityUpdatedEvent))),

      DataSink("ccl-in-review-downloadable-resource-planned-in-courses-sink", filterInReview(getDfWithHeader(downloadableResourcePlannedSchema, DownloadableResourcePlannedEvent))),
      DataSink("ccl-in-review-downloadable-resource-unplanned-in-courses-sink", filterInReview(getDfWithHeader(downloadableResourceUnplannedSchema, DownloadableResourceUnplannedEvent))),
      DataSink("ccl-in-review-downloadable-resource-updated-in-courses-sink", filterInReview(getDfWithHeader(downloadableResourceUpdatedSchema, DownloadableResourceUpdatedEvent))),
      DataSink("ccl-published-downloadable-resource-planned-in-courses-sink", filterPublished(getDfWithHeader(downloadableResourcePlannedSchema, DownloadableResourcePlannedEvent))),
      DataSink("ccl-published-downloadable-resource-unplanned-in-courses-sink", filterPublished(getDfWithHeader(downloadableResourceUnplannedSchema, DownloadableResourceUnplannedEvent))),
      DataSink("ccl-published-downloadable-resource-updated-in-courses-sink", filterPublished(getDfWithHeader(downloadableResourceUpdatedSchema, DownloadableResourceUpdatedEvent))),
    )
  }
}

object CourseEvents {

  val JobName = "ccl-course-events"

  def main(args: Array[String]): Unit = {
    new CourseEvents(SparkSessionUtils.getSession(JobName)).run
  }
}