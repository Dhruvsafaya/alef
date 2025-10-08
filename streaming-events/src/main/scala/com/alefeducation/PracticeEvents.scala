package com.alefeducation

import com.alefeducation.schema.Schema._
import com.alefeducation.service.{DataSink, SparkStreamingService}
import com.alefeducation.util.Constants._
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.Resources.getPayloadStream
import com.alefeducation.util.{DateField, SparkSessionUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, TimestampType}

class PracticeEventsTransformer(override val name: String, override val session: SparkSession) extends SparkStreamingService {

  override def transform(): List[DataSink] = {
    val kafkaStream = read("practice-source", session)
    val payloadStream = getPayloadStream(session, kafkaStream)
    val dateField = DateField("occurredOn", TimestampType)
    val dateOccurredOnEpoch = DateField("occurredOn", LongType)

    List(
      DataSink("practice-created-sink", process(session, payloadStream, practiceCreatedSchema, List(PracticeCreated), dateField)),
      DataSink("practice-session-started-sink",
               process(session, payloadStream, practiceSessionStartedSchema, List(PracticeSessionStarted), dateField)),
      DataSink("practice-session-finished-sink",
               process(session, payloadStream, practiceSessionFinishedSchema, List(PracticeSessionFinished), dateField)),
      DataSink("practice-item-session-started-sink",
               process(session, payloadStream, practiceItemSessionStartedSchema, List(PracticeItemSessionStarted), dateField)),
      DataSink("practice-item-session-finished-sink",
               process(session, payloadStream, practiceItemSessionFinishedSchema, List(PracticeItemSessionFinished), dateField)),
      DataSink(
        "practice-item-content-session-started-sink",
        process(session, payloadStream, practiceItemContentSessionStartedSchema, List(PracticeItemContentSessionStarted), dateField)
      ),
      DataSink(
        "practice-item-content-session-finished-sink",
        process(session, payloadStream, practiceItemContentSessionFinishedSchema, List(PracticeItemContentSessionFinished), dateField)
      ),
      DataSink("teacher-feedback-sent-sink",
               process(session, payloadStream, teacherFeedbackSentSchema, List(TeacherFeedbackSentEvent), dateOccurredOnEpoch)),
      DataSink("teacher-message-sent-sink",
               process(session, payloadStream, teacherMessageSentSchema, List(TeacherMessageSentEvent), dateOccurredOnEpoch)),
      DataSink("guardian-feedback-read-sink",
               process(session, payloadStream, guardianFeedbackReadSchema, List(GuardianFeedbackReadEvent), dateOccurredOnEpoch)),
      DataSink("guardian-message-sent-sink",
               process(session, payloadStream, guardianMessageSentSchema, List(GuardianMessageSentEvent), dateOccurredOnEpoch)),
      DataSink("teacher-message-deleted-sink",
               process(session, payloadStream, teacherMessageDeletedSchema, List(TeacherMessageDeletedEvent), dateOccurredOnEpoch)),
      DataSink("teacher-feedback-deleted-sink",
               process(session, payloadStream, teacherFeedbackDeletedSchema, List(TeacherFeedbackDeletedEvent), dateOccurredOnEpoch)),
      DataSink("adt-next-question-sink",
               process(session, payloadStream, adtNextQuestionSchema, List(ADTNextQuestionEvent), dateOccurredOnEpoch)),
      DataSink("adt-student-report-sink",
               process(session, payloadStream, adtStudentReportSchema, List(ADTStudentReportEvent), dateOccurredOnEpoch)),
      DataSink("adt-attempt-threshold-mutated-sink",
        process(session, payloadStream, adtAttemptThresholdCreatedSchema, List(AttemptThresholdCreatedEvent, AttemptThresholdUpdatedEvent), dateOccurredOnEpoch)),
      DataSink("teacher-announcement-sink",
               process(session, payloadStream, teacherAnnouncementSchema, List(TeacherAnnouncementSentEvent), dateOccurredOnEpoch)),
      DataSink("principal-announcement-sink",
               process(session, payloadStream, principalAnnouncementSchema, List(PrincipalAnnouncementSentEvent), dateOccurredOnEpoch)),
      DataSink("superintendent-announcement-sink",
               process(session, payloadStream, superintendentAnnouncementSchema, List(SuperintendentAnnouncementSentEvent), dateOccurredOnEpoch)),
      DataSink("announcement-deleted-sink",
               process(session, payloadStream, announcementDeletedSchema, List(AnnouncementDeletedEvent), dateOccurredOnEpoch)),
      DataSink("guardian-joint-activity-pending-sink",
        process(session, payloadStream, guardianJointPendingActivitySchema, List(GuardianJointActivityPendingEvent), dateOccurredOnEpoch)),
      DataSink("guardian-joint-activity-started-sink",
        process(session, payloadStream, guardianJointStartedActivitySchema, List(GuardianJointActivityStartedEvent), dateOccurredOnEpoch)),
      DataSink("guardian-joint-activity-completed-sink",
        process(session, payloadStream, guardianJointCompletedActivitySchema, List(GuardianJointActivityCompletedEvent), dateOccurredOnEpoch)),
      DataSink("guardian-joint-activity-rated-sink",
        process(session, payloadStream, guardianJointRatedActivitySchema, List(GuardianJointActivityRatedEvent), dateOccurredOnEpoch))
    )
  }
}

object PracticeEvents {
  def main(args: Array[String]): Unit = {
    val name = "practice-events"
    new PracticeEventsTransformer(name, SparkSessionUtils.getSession(name)).run
  }
}
