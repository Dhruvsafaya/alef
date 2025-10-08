package com.alefeducation.dimensions.interim_checkpoint

import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.interim_checkpoint.InterimCheckpointTransformer.transformForCreate
import com.alefeducation.dimensions.interim_checkpoint.InterimCheckpointLessonAssociationTransformer.transformForICLessonAssociation
import com.alefeducation.dimensions.interim_checkpoint.InterimCheckpointRulesTransformer.transformForIcRules
import com.alefeducation.service.SparkBatchService
import com.alefeducation.util.DataFrameUtility._
import com.alefeducation.util.{DateField, SparkSessionUtils}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.LongType

import scala.collection.immutable.Map

class InterimCheckpointDimension(override implicit val session: SparkSession) extends SparkBatchService {

  import InterimCheckpointDimension._
  import com.alefeducation.util.BatchTransformerUtility._
  override val name: String = JobName

  override def transform(): List[Sink] = {

    // Interim Checkpoint Rules Created event, extract different association from this
    val createdDF = readParquetAndTransformOccurredOn(ParquetInterimCheckpointSource)
    val updatedDF = readParquetAndTransformOccurredOn(ParquetInterimCheckpointUpdatedSource)
    val deletedDF = readParquetAndTransformOccurredOn(ParquetPublishedCheckpointDeletedSource)
    val interimCheckPointDeletedDf = readParquetAndTransformOccurredOn(ParquetInterimCheckpointDeletedSource)
    val sinks = transformForCreate(createdDF, updatedDF, deletedDF, interimCheckPointDeletedDf)

    val latestUpdatedDf = updatedDF.map(_.selectLatestByRowNumber(List("interimCheckpointId")))
    // Interim Checkpoint Lesson Association
    val icLessonAssociationSinks = transformForICLessonAssociation(createdDF, latestUpdatedDf)

    // Interim Checkpoint Rules
    val icRulesSinks = transformForIcRules(createdDF, latestUpdatedDf)

    sinks ++ icLessonAssociationSinks ++ icRulesSinks
  }

  private def readParquetAndTransformOccurredOn(source: String): Option[DataFrame] = {
    readOptional(source, session, isMandatory = false).map {
      addOccurredOnColumn(session, DateField("occurredOn", LongType), _)
    }
  }
}

object InterimCheckpointDimension {

  val JobName: String = "interim-checkpoint-dimension"

  val InterimCheckpointPrefix = "ic"
  val InterimCheckpointLessonPrefix = "ic_lesson"
  val InterimCheckpointRulePrefix = "ic_rule"
  val InterimCheckpointRulesCreatedEvent = "InterimCheckpointRulesCreatedEvent"
  val InterimCheckpointRulesUpdatedEvent = "InterimCheckpointRulesUpdatedEvent"
  val PublishedCheckpointDeletedEvent = "PublishedCheckpointDeletedEvent"
  val ParquetInterimCheckpointSource = "parquet-interim-checkpoint-source"
  val ParquetInterimCheckpointUpdatedSource = "parquet-interim-checkpoint-updated-source"
  val ParquetInterimCheckpointDeletedSource = "parquet-interim-checkpoint-deleted-source"
  val ParquetPublishedCheckpointDeletedSource = "parquet-published-checkpoint-deleted-source"
  val ParquetInterimCheckpointDeletedSink = "parquet-interim-checkpoint-deleted-sink"
  val ParquetPublishedCheckpointDeletedSink = "parquet-published-checkpoint-deleted-sink"
  val ParquetInterimCheckpointSink = "parquet-interim-checkpoint-sink"
  val ParquetInterimCheckpointUpdatedSink = "parquet-interim-checkpoint-updated-sink"
  val RedshiftInterimCheckpointSink = "redshift-interim-checkpoint-sink"
  val RedshiftInterimCheckpointDeletedSink = "redshift-interim-checkpoint-deleted-sink"
  val RedshiftInterimCheckpointRulesSink = "redshift-interim-checkpoint-rules-sink"
  val RedshiftInterimCheckpointLessonSink = "redshift-ic-lesson-sink"
  val DeltaInterimCheckpointSink = "delta-interim-checkpoint-sink"
  val DeltaInterimCheckpointDeleteSink = "delta-interim-checkpoint-delete-sink"
  val DeltaInterimCheckpointRulesSink = "delta-interim-checkpoint-rules-sink"
  val DeltaInterimCheckpointLessonSink = "delta-ic-lesson-sink"
  val IcMaterialType = "ic_material_type"
  val MaterialType = "materialType"
  val InstructionPlan = "INSTRUCTIONAL_PLAN"

  val interimCheckpointCols = Map(
    "interimCheckpointId" -> "ic_id",
    "title" -> "ic_title",
    "language" -> "ic_language",
    "ic_status" -> "ic_status",
    "occurredOn" -> "occurredOn",
    IcMaterialType -> IcMaterialType
  )

  val interimCheckpointDeleteCols = Map(
    "id" -> "ic_id",
    "ic_status" -> "ic_status",
    "occurredOn" -> "occurredOn"
  )

  val interimCheckpointLessonCols = Map(
    "interimCheckpointId" -> "ic_uuid",
    "lessonId" -> "lo_uuid",
    "ic_lesson_status" -> "ic_lesson_status",
    "ic_lesson_attach_status" -> "ic_lesson_attach_status",
    "ic_lesson_type" -> "ic_lesson_type",
    "occurredOn" -> "occurredOn"
  )

  val interimCheckpointRulesCols = Map(
    "interimCheckpointId" -> "ic_uuid",
    "learningOutcomeId" -> "outcome_uuid",
    "resourceType" -> "ic_rule_resource_type",
    "numQuestions" -> "ic_rule_no_questions",
    "ic_rule_type" -> "ic_rule_type",
    "ic_rule_status" -> "ic_rule_status",
    "ic_rule_attach_status" -> "ic_rule_attach_status",
    "occurredOn" -> "occurredOn"
  )

  def apply(implicit session: SparkSession): InterimCheckpointDimension = new InterimCheckpointDimension

  def main(args: Array[String]): Unit = {
    InterimCheckpointDimension(SparkSessionUtils.getSession(JobName)).run
  }

}
