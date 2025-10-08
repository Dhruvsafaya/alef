package com.alefeducation

import com.alefeducation.schema.Schema._
import com.alefeducation.service.{DataSink, SparkStreamingService}
import com.alefeducation.util.DataFrameUtility.process
import com.alefeducation.util.{DateField, SparkSessionUtils}
import com.alefeducation.util.Resources.getPayloadStream
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, TimestampType}

class TutorRawEvents(override val session: SparkSession) extends SparkStreamingService {

  import TutorRawEvents._

  override val name: String = TutorRawEvents.name

  override def transform(): List[DataSink] = {
    val kafkaStream = read("tutor-events-source", session)
    val payloadStream = getPayloadStream(session, kafkaStream)
    val dateOccurredOnTs = DateField("occurredOn", TimestampType)

    val sinks = List(
      DataSink("tutor-context-sink",
        process(session, payloadStream, tutorContextSchema, List(UserContextCreated), dateOccurredOnTs)),
      DataSink("tutor-session-sink",
        process(session, payloadStream, tutorSessionSchema, List(ChatSession), dateOccurredOnTs)),
      DataSink("tutor-conversation-sink",
        process(session, payloadStream, tutorConversationSchema, List(ChatConversationOccurred), dateOccurredOnTs)),
      DataSink("tutor-suggestions-sink",
        process(session, payloadStream, tutorSuggestionsSchema, List(ChatSuggestionsGenerated), dateOccurredOnTs)),
      DataSink("tutor-onboarding-sink",
        process(session, payloadStream, tutorOnboardingSchema, List(OnboardingQuestionAnswered), dateOccurredOnTs)),
      DataSink("tutor-calltoaction-sink",
        process(session, payloadStream, tutorCallToActionSchema, List(CallToActionAnswered), dateOccurredOnTs)),
      DataSink("tutor-translation-sink",
        process(session, payloadStream, tutorTranslationSchema, List(TranslationOccurred), dateOccurredOnTs)),
      DataSink("tutor-analogous-sink",
        process(session, payloadStream, tutorAnalogousSchema, List(AnalogousOccurred), dateOccurredOnTs)),
      DataSink("tutor-simplification-sink",
        process(session, payloadStream, tutorSimplificationSchema, List(SimplificationOccurred), dateOccurredOnTs)),
      DataSink("tutor-challengequestions-sink", process(session, payloadStream, tutorChallengeQuestionsSchema,
        List(ChallengeQuestionGenerated), dateOccurredOnTs)),
      DataSink("tutor-challengequestionevalution-sink", process(session, payloadStream, tutorChallengeQuestionEvaluationSchema,
        List(ChallengeQuestionEvaluated), dateOccurredOnTs))
    )
    sinks
  }
}


object TutorRawEvents {

  private val name = "tutor-events"
  final val UserContextCreated = "UserContextUpdated"
  final val ChatSession = "ChatSessionUpdated"
  final val ChatConversationOccurred = "ChatConversationOccurred"
  final val ChatSuggestionsGenerated = "ChatSuggestionsGenerated"
  final val OnboardingQuestionAnswered = "OnboardingQuestionAnswered"
  final val CallToActionAnswered = "CallToActionAnswered"
  final val TranslationOccurred = "TranslationOccurred"
  final val AnalogousOccurred = "AnalogousOccurred"
  final val SimplificationOccurred = "SimplificationOccurred"
  final val ChallengeQuestionGenerated = "ChallengeQuestionGenerated"
  final val ChallengeQuestionEvaluated = "ChallengeQuestionEvaluated"

  def main(args: Array[String]): Unit = {
    new TutorRawEvents(SparkSessionUtils.getSession(name)).run
  }
}
