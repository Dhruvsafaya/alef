package com.alefeducation.facts

import com.alefeducation.bigdata.Sink
import com.alefeducation.bigdata.batch.delta.{Delta, DeltaSink}
import com.alefeducation.service.{DataSink, SparkBatchService}
import com.alefeducation.util.Helpers._
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class NoSkillContent(override val name: String, override implicit val session: SparkSession) extends SparkBatchService {
  import session.implicits._
  import Delta.Transformations._
  import NoSkillContent._
  import com.alefeducation.util.BatchTransformerUtility._

  override def transform(): List[Sink] = {

    val noSkillContentOptional = readOptional(NoSkillContentSource, session)

    val transformed = noSkillContentOptional
      .map(
        _.withColumn("practiceSkills", explode($"recommendedSkillsWithNoMlo"))
          .withColumn("skillId", $"practiceSkills.uuid"))
      .map(_.selectColumnsWithMapping(StagingNoSkillContent)
        .appendTimestampColumns(NoSkillContentEntity, isFact = true))

    val parquetSink: Option[DataSink] = noSkillContentOptional.map(_.toParquetSink(NoSkillContentSource))
    val redshiftSink: Option[DataSink] = transformed.map(_.toRedshiftInsertSink(RedshiftNoSkillContentSink, "NoMloForSkillsFound"))

    val deltaSink: Option[DeltaSink] = transformed.flatMap { df =>
      val selCols = df.columns.map(
        k =>
          if (k.contains("uuid")) col(k).alias("scu_" + k.replace("uuid", "id"))
          else col(k))
      df.select(selCols: _*)
        .withColumn("eventdate", date_format(col("scu_created_time"), "yyyy-MM-dd"))
        .toCreate(isFact = true)
        .map(_.toSink(DeltaSinkName, eventType = "NoMloForSkillsFound"))
    }

    (parquetSink ++ redshiftSink ++ deltaSink).toList
  }
}

object NoSkillContent {
  val DeltaSinkName = "delta-no-skill-content-sink"
  def main(args: Array[String]): Unit = {
    new NoSkillContent(NoSkillContentService, SparkSessionUtils.getSession(NoSkillContentService)).run
  }
}
