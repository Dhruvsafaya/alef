package com.alefeducation.facts

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Delta
import com.alefeducation.facts.learning_experience.transform.LearningExperienceAggregation.DeltaSinkName
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.BatchUtility.getParquetSink
import com.alefeducation.util.DataFrameUtility.{addTimestampColumns, isEmpty, selectAs}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.Constants._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

case class DataSinkInput(inputDf: DataFrame, parquetSource: String)

class LessonFeedbackTransformer(override val name: String, override val session: SparkSession) extends SparkBatchService {

  import session.implicits._
  import Delta.Transformations._

  override def transform(): List[Sink] = {

    val lessonFeedback = read(ParquetLessonFeedbackSource, session)

    if (!isEmpty(lessonFeedback)) {
      val feedbackDf = lessonFeedback
        .withColumn("rating", when($"rating".isNull, lit(MinusOne)).otherwise($"rating"))
        .withColumn("ratingText", when($"ratingText".isNull, lit(DefaultStringValue)).otherwise(trim($"ratingText")))
        .withColumn("hasComment", when($"comment".isNull, lit(false)).otherwise(lit(true)))
        .withColumn("isCancelled", when($"eventType".equalTo(LessonFeedbackSubmitted), lit(false)).otherwise(lit(true)))
      val redshiftLessonFeedbackDf = selectAs(feedbackDf, StagingLessonFeedback)
      val withTimeStamp = addTimestampColumns(redshiftLessonFeedbackDf, LessonFeedbackEntity, isFact = true)

      val selectFieldsForDelta = StagingLessonFeedback.map { case (k, v) => (k, v.replace("_uuid", "_id")) } ++ Map(
        "comment" -> s"${LessonFeedbackEntity}_comment",
        "occurredOn" -> "occurredOn"
      )
      val deltaLessonFeedbackWithComment = selectAs(feedbackDf, selectFieldsForDelta)
        .withColumn("eventdate", date_format($"occurredOn", "yyyy-MM-dd"))
      val deltaWithTimeStamp = addTimestampColumns(deltaLessonFeedbackWithComment, LessonFeedbackEntity, isFact = true)
      val deltaFeedbackSink = deltaWithTimeStamp.toCreate(isFact = true).map(_.toSink(DeltaLessonFeedbackSink))

      val sink = getParquetSink(session, ParquetLessonFeedbackSource, lessonFeedback)
      List(sink, DataSink(RedshiftLessonFeedbackSink, withTimeStamp)) ++ deltaFeedbackSink
    } else Nil
  }
}

object LessonFeedback {
  def main(args: Array[String]): Unit = {
    new LessonFeedbackTransformer(LessonFeedbackService, SparkSessionUtils.getSession(LessonFeedbackService)).run
  }
}
