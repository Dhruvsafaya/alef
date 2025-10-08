package com.alefeducation.facts.tutor

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.tutor.TutorConversationTransform._
import com.alefeducation.facts.tutor.TutorSessionTransform.TutorSessionTransformService
import com.alefeducation.service.DataSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{LongType, StringType, TimestampType}


class TutorConversationTransform(val session: SparkSession,
                                 val service: SparkBatchService) {

  import com.alefeducation.util.BatchTransformerUtility._

  def transform(): Option[DataSink] = {
    val tutorConversationParquet = service.readOptional(ParquetTutorConversationSource, session)

    val tutorConversationTransformed = tutorConversationParquet.map(
        _.addColIfNotExists("sessionStatus", StringType)
        .transformForInsertFact(TutorConversationCols, FactTutorConversationEntity)
        .transform(sessionStatus(FactTutorConversationEntity))
        .withColumn("ftc_user_message_timestamp", col("ftc_user_message_timestamp").cast(TimestampType))
        .withColumn("ftc_bot_message_timestamp", col("ftc_bot_message_timestamp").cast(TimestampType))
        .withColumn("ftc_subject_id", col("ftc_subject_id").cast(LongType))
    )

    tutorConversationTransformed.map(df => {
      DataSink(TransformTutorConversationSink, df)
    })
  }


}


object TutorConversationTransform {
  val TutorConversationTransformService = "transform-tutor-conversation"
  val ParquetTutorConversationSource = "parquet-tutor-conversation-source"
  val FactTutorConversationEntity = "ftc"
  val TransformTutorConversationSink = "tutor-conversation-transformed-sink"

  val TutorConversationCols = Map[String, String](
    "userId" -> "ftc_user_id",
    "role" -> "ftc_role",
    "contextId" -> "ftc_context_id",
    "schoolId" -> "ftc_school_id",
    "gradeId" -> "ftc_grade_id",
    "grade" -> "ftc_grade",
    "subjectId" -> "ftc_subject_id",
    "subject" -> "ftc_subject",
    "language" -> "ftc_language",
    "materialId" -> "ftc_material_id",
    "materialType" -> "ftc_material_type",
    "activityId" -> "ftc_activity_id",
    "activityStatus" -> "ftc_activity_status",
    "levelId" -> "ftc_level_id",
    "outcomeId" -> "ftc_outcome_id",
    "occurredOn" -> "occurredOn",
    "tenantId" -> "ftc_tenant_id",
    "sessionId" -> "ftc_session_id",
    "sessionStatus" -> "ftc_session_state",
    "conversationId" -> "ftc_conversation_id",
    "conversationMaxTokens" ->  "ftc_conversation_max_tokens",
    "conversationTokenCount" ->  "ftc_conversation_token_count",
    "systemPrompt" ->  "ftc_system_prompt",
    "systemPromptTokens" ->  "ftc_system_prompt_tokens",
    "messageId" -> "ftc_message_id",
    "messageLanguage" ->  "ftc_message_language",
    "messageFeedback" ->  "ftc_message_feedback",
    "messageTokens" -> "ftc_message_tokens",
    "userMessage" ->  "ftc_user_message",
    "userMessageSource" ->  "ftc_user_message_source",
    "userMessageTokens" ->  "ftc_user_message_tokens",
    "userMessageTimestamp" ->  "ftc_user_message_timestamp",
    "botMessage" ->  "ftc_bot_message",
    "botMessageSource" ->  "ftc_bot_message_source",
    "botMessageTokens" ->  "ftc_bot_message_tokens",
    "botMessageTimestamp" ->  "ftc_bot_message_timestamp",
    "botMessageConfidence" ->  "ftc_bot_message_confidence",
    "botMessageResponseTime" ->  "ftc_bot_message_response_time" ,
    "suggestionsPrompt" -> "ftc_suggestions_prompt",
    "suggestionsPromptTokens" -> "ftc_suggestions_prompt_tokens",
    "activityPageContextId" -> "ftc_activity_page_context_id",
    "studentLocation" -> "ftc_student_location",
    "suggestionClicked" -> "ftc_suggestion_clicked",
    "clickedSuggestionId" -> "ftc_clicked_suggestion_id"
  )

  val session: SparkSession = SparkSessionUtils.getSession(TutorSessionTransformService)
  val service = new SparkBatchService(TutorConversationTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new TutorConversationTransform(session, service)
    service.run(transformer.transform())
  }
}