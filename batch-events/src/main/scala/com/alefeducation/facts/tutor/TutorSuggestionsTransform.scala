package com.alefeducation.facts.tutor

import com.alefeducation.base.SparkBatchService
import com.alefeducation.facts.tutor.TutorSuggestionsTransform.{FactTutorSuggestionsEntity, TutorSuggestionsCols, TutorSuggestionsKey, suggestionssinkName, suggestionssourceName}
import com.alefeducation.schema.internal.ControlTableUtils.ProductMaxIdType
import com.alefeducation.service.DataSink
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, explode}

class TutorSuggestionsTransform(val session: SparkSession,
                       val service: SparkBatchService) {

  def transform(): Option[DataSink] = {
    val tutorSuggestionsParquet: Option[DataFrame] = service.readOptional(suggestionssourceName, session, extraProps = List(("mergeSchema", "true")))

    val startId: Long = service.getStartIdUpdateStatus(TutorSuggestionsKey)

    val tutorSuggestionsTransformed: Option[DataFrame] = tutorSuggestionsParquet.map(
      _.withColumn("suggestion", explode(col("suggestions")))
        .withColumn("fts_suggestion_id", col("suggestion.suggestionId"))
        .withColumn("fts_suggestion_question", col("suggestion.suggestionQuestion"))
        .withColumn("fts_suggestion_clicked", col("suggestion.suggestionClicked"))
        .drop("suggestions").drop("suggestion")
        .transformForInsertFact(TutorSuggestionsCols, FactTutorSuggestionsEntity)
        .genDwId(s"${FactTutorSuggestionsEntity}_dw_id", startId)
      )

    tutorSuggestionsTransformed.map(df => {
      DataSink(suggestionssinkName, df, controlTableUpdateOptions = Map(ProductMaxIdType -> TutorSuggestionsKey))})
  }
}

object TutorSuggestionsTransform {
  val TutorSuggestionsTransformService = "transform-tutor-suggestions"

  val FactTutorSuggestionsEntity = "fts"
  val TutorSuggestionsKey = "fact_tutor_suggestions"

  val suggestionssourceName = getSource(TutorSuggestionsTransformService).head
  val suggestionssinkName = getSink(TutorSuggestionsTransformService).head

  val TutorSuggestionsCols = Map[String, String](
      "messageId" -> "fts_message_id",
      "fts_suggestion_id" -> "fts_suggestion_id",
      "userId" -> "fts_user_id",
      "sessionId" -> "fts_session_id",
      "conversationId" -> "fts_conversation_id",
      "responseTime" -> "fts_response_time",
      "successParserTokens" -> "fts_success_parser_tokens",
      "failureParserTokens" -> "fts_failure_parser_tokens",
      "fts_suggestion_question" -> "fts_suggestion_question",
      "fts_suggestion_clicked" -> "fts_suggestion_clicked",
      "occurredOn" -> "occurredOn",
      "tenantId" -> "fts_tenant_id"
  )

  val session: SparkSession = SparkSessionUtils.getSession(TutorSuggestionsTransformService)
  val service = new SparkBatchService(TutorSuggestionsTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new TutorSuggestionsTransform(session, service)
    service.run(transformer.transform())
  }
}
