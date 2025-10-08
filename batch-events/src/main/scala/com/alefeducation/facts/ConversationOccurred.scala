package com.alefeducation.facts

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Delta
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.Helpers.{FactConversationOccurredEntity => FCO, _}
import com.alefeducation.util.Constants.{ConversationOccurred => ConversationOccurredEventType}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

class ConversationOccurred(override val name: String, override val session: SparkSession) extends SparkBatchService {

  import Delta.Transformations._
  import com.alefeducation.util.BatchTransformerUtility._

  override def transform(): List[Sink] = {
    val conversationOccurred = readOptional(ParquetConversationOccurredSource, session)

    val transConversationOccurred = conversationOccurred.map(
      _.selectColumnsWithMapping(StagingConversationOccurred)
        .transform(addCols)
        .appendEventDate
        .appendTimestampColumns(FCO, isFact = true)
    )

    val parquetSink = conversationOccurred.map(_.toParquetSink(ParquetConversationOccurredSink))
    val redshiftSink = transConversationOccurred.map(_.toRedshiftInsertSink(RedshiftConversationOccurredSink, ConversationOccurredEventType))
    val deltaSink = transConversationOccurred.map(_.renameUUIDcolsForDelta(Some(s"${FCO}_")))
      .flatMap(_.toCreate(isFact = true).map(_.toSink(DeltaConversationOccurredSink)))

    (parquetSink ++ redshiftSink ++ deltaSink).toList
  }

  def addCols(df: DataFrame): DataFrame =
    df.withColumn(s"${FCO}_suggestions",
        when(size(col(s"${FCO}_suggestions")) > ZeroArraySize,
          substring(concat_ws(", ", col(s"${FCO}_suggestions")), StartingIndexConversationOccurred, EndIndexConversationOccurred)
        ).otherwise("n/a"))
      .withColumn(s"${FCO}_question", substring(col(s"${FCO}_question"), StartingIndexConversationOccurred, EndIndexConversationOccurred))
      .withColumn(s"${FCO}_answer", substring(col(s"${FCO}_answer"), StartingIndexConversationOccurred, EndIndexConversationOccurred))
      .withColumn(s"${FCO}_arabic_answer", substring(col(s"${FCO}_arabic_answer"), StartingIndexConversationOccurred, EndIndexConversationOccurred))

}

object ConversationOccurred {
  def main(args: Array[String]): Unit = {
    new ConversationOccurred(ConversationOccurredService, SparkSessionUtils.getSession(ConversationOccurredService)).run
  }
}