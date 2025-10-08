package com.alefeducation

import com.alefeducation.schema.Schema._
import com.alefeducation.service.{DataSink, SparkStreamingService}
import com.alefeducation.util.Constants.{EbookProgressEvent, ExperienceSubmitted, LearningContentFinished, LearningContentSkipped, LearningContentStarted, LevelCompletedEvent, LevelsRecommendedEvent, PathwayActivityCompletedEvent, TotalScoreUpdatedEvent}
import com.alefeducation.util.DataFrameUtility.{process, _}
import com.alefeducation.util.Resources._
import com.alefeducation.util.{Constants, DateField, SparkSessionUtils}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, TimestampType}

class LearningProgressRawEventsTransformer(override val name: String, override val session: SparkSession) extends SparkStreamingService() {

  override def transform(): List[DataSink] = {
    val kafkaStream = read(LearningProgressRawEvents.source, session)
    val payloadStream = getPayloadStream(session, kafkaStream)
    val dateField = DateField("occurredOn", TimestampType)
    val dateFieldEpoch = DateField("occurredOn", LongType)

    val progressEvents = List(
      DataSink("learning-session-started-sink",
        process(session, payloadStream, learningSessionStartedSchema, List(Constants.LearningSessionStarted), dateField)),
      DataSink("learning-experience-started-sink",
        process(session, payloadStream, experienceStartedSchema, List(Constants.ExperienceStarted), dateField)),
      DataSink("learning-experience-finished-sink",
        process(session, payloadStream, experienceFinishedSchema, List(Constants.ExperienceFinished), dateField)),
      DataSink("learning-session-finished-sink",
        process(session, payloadStream, learningSessionFinishedSchema, List(Constants.LearningSessionFinished), dateField)),
      DataSink("experience-submitted-sink",
        process(session, payloadStream, experienceSubmittedSchema, List(ExperienceSubmitted), dateField)),
      DataSink("learning-content-started-sink",
        process(session, payloadStream, learningContentStartFinishSchema, List(LearningContentStarted), dateField)),
      DataSink("learning-content-skipped-sink",
        process(session, payloadStream, learningContentStartFinishSchema, List(LearningContentSkipped), dateField)),
      DataSink("learning-content-finished-sink",
        process(session, payloadStream, learningContentFinsihedSchema, List(LearningContentFinished), dateField)),
      DataSink("learning-total-score-updated-sink",
        process(session, payloadStream, totalScoreUpdatedSchema, List(TotalScoreUpdatedEvent), dateField)),
      DataSink("learning-experience-discarded-sink",
        process(session, payloadStream, experienceDiscardedSchema, List(Constants.ExperienceDiscarded), dateField)),
      DataSink("learning-session-deleted-sink",
        process(session, payloadStream, sessionDeletedSchema, List(Constants.SessionDeleted), dateField)),
      DataSink("learning-core-additional-resources-assigned-sink",
        process(session, payloadStream, CoreAdditionalResourcesAssignedSchema, List(Constants.CoreAdditionalResourcesAssignedEvent), dateField)),
      DataSink(
        "learning-ebook-progress-sink",
        process(session, payloadStream, ebookProgressEventSchema, List(EbookProgressEvent), dateFieldEpoch))
    )
    progressEvents
  }
}

object LearningProgressRawEvents {

  val name = "learning-session"
  val source = "learning-session-source"

  def main(args: Array[String]): Unit = {
    new LearningProgressRawEventsTransformer(name, SparkSessionUtils.getSession(name)).run
  }
}
