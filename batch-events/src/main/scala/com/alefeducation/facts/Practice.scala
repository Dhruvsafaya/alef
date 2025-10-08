package com.alefeducation.facts

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.Delta
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.BatchUtility.getParquetSink
import com.alefeducation.util.DataFrameUtility.{addTimestampColumns, isEmpty, selectAs}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class PracticeTransformer(override val name: String, override val session: SparkSession) extends SparkBatchService {

  import Delta.Transformations._
  import session.implicits._
  override def transform(): List[Sink] = {

    val practiceCreatedDF = read(ParquetPracticeSource, session)
    if (!isEmpty(practiceCreatedDF)) {
      val practiceDF = practiceCreatedDF
        .withColumn("occurredOn", regexp_replace($"occurredOn", "T", " "))
        .withColumn("practiceItem", explode($"items"))
        .withColumn("practiceSkills", explode($"skills"))

      val practiceItemFlattenedDF = practiceDF
        .withColumn("practiceItemLearningObjectiveId", $"practiceItem.learningObjectiveId")
        .withColumn("practiceItemContent", explode($"practiceItem.contents"))
        .withColumn("practiceItemSkill", explode($"practiceItem.skills"))

      val practiceItemContentFlattenedDF = practiceItemFlattenedDF
        .withColumn("skillId", $"practiceSkills.uuid")
        .withColumn("practiceItemSkillId", $"practiceItemSkill.uuid")
        .withColumn("practiceItemContentId", $"practiceItemContent.id")
        .withColumn("practiceItemContentTitle", $"practiceItemContent.title")
        .withColumn("practiceItemContentLessonType", $"practiceItemContent.lessonType")
        .withColumn("practiceItemContentLocation", $"practiceItemContent.location")
      val redshiftPracticeDf =
        addTimestampColumns(selectAs(practiceItemContentFlattenedDF, StagingPractice), FactPractice, isFact = true)
      val sink = getParquetSink(session, ParquetPracticeSource, practiceCreatedDF)
      val deltaPracticeSink = getDeltaSink(redshiftPracticeDf)
      List(
        sink,
        DataSink(RedshiftPracticeSink, redshiftPracticeDf)
      ) ++ deltaPracticeSink
    } else Nil
  }

  private def getDeltaSink(practiceDf: DataFrame): Option[Sink] = {

    val practiceDeltaDf = selectAs(practiceDf, PracticeDeltaColumns)
      .withColumn("eventdate", date_format($"practice_created_time", "yyyy-MM-dd"))
    practiceDeltaDf.toCreate(isFact = true).map(_.toSink(DeltaPracticeSink))
  }

}

object Practice {
  def main(args: Array[String]): Unit = {
    new PracticeTransformer(PracticeService, SparkSessionUtils.getSession(PracticeService)).run
  }
}
