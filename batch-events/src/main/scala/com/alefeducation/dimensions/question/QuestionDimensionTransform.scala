package com.alefeducation.dimensions.question

import com.alefeducation.base.SparkBatchService
import com.alefeducation.bigdata.Sink
import com.alefeducation.dimensions.question.QuestionDimensionTransform.{QuestionIWHCols, QuestionMutatedEntityPrefix, QuestionMutatedSinkName, QuestionMutatedSourceName}
import com.alefeducation.schema.question.Question
import com.alefeducation.service.DataSink
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.catalyst.ScalaReflection.schemaFor
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class QuestionDimensionTransform(val session: SparkSession, val service: SparkBatchService) {

  def transform(): List[Option[Sink]] = {

    import com.alefeducation.util.BatchTransformerUtility._

    val questionsDF : Option[DataFrame] = service.readOptional(QuestionMutatedSourceName, session, extraProps = List(("mergeSchema", "true")))

    val metadataSchema = schemaFor[Question].dataType.asInstanceOf[StructType]

    val window = Window.partitionBy("questionId").orderBy(col("version"))

    val transformedDf = questionsDF.map(
       _.dropDuplicates()
      .withColumn("question_metadata", from_json(col("metadata"), metadataSchema))
      .withColumn("question_active_until", (lead("occurredOn", 1).over(window)).cast(TimestampType))
      .withColumn("question_authored_date_ts", col("question_metadata.authoredDate").cast(TimestampType))
    )

    val IWHDf = transformedDf.map(
        _.transformForIWH2(
          QuestionIWHCols,
          QuestionMutatedEntityPrefix,
          0, //unnecessary field
          List("QuestionCreatedEvent","QuestionUpdatedEvent"),
          Nil,
          List("questionId"),
          inactiveStatus = 2
        )
    )

    List(
      IWHDf.map(DataSink(QuestionMutatedSinkName, _))
    )
  }

}

object QuestionDimensionTransform {


  private val QuestionMutatedTransformService = "transformed-ccl-question-mutated-service"

  val QuestionMutatedEntityPrefix = "question"

  val QuestionMutatedSourceName: String = getSource(QuestionMutatedTransformService).head
  val QuestionMutatedSinkName: String = getSink(QuestionMutatedTransformService).head

  val session: SparkSession = SparkSessionUtils.getSession(QuestionMutatedTransformService)
  val service = new SparkBatchService(QuestionMutatedTransformService, session)

  def main(args: Array[String]): Unit = {
    val transformer = new QuestionDimensionTransform(session, service)
    service.runAll(transformer.transform().flatten)
  }

  val QuestionIWHCols: Map[String, String] = Map[String, String](
    "questionId" -> "question_id",
    "code" -> "question_code",
    "version" -> "question_version",
    "type" -> "question_type",
    "variant" -> "question_variant",
    "language" -> "question_language",
    "body" -> "question_body",
    "validation" -> "question_validation",
    "stage" -> "question_stage",
    "maxScore" -> "question_max_score",
    "triggeredBy" -> "question_triggered_by",
    "question_metadata.curriculumOutcomes" -> "question_curriculum_outcomes",
    "question_metadata.keywords" -> "question_keywords",
    "question_metadata.formatType" -> "question_format_type",
    "question_metadata.conditionsOfUse" -> "question_conditions_of_use",
    "question_metadata.resourceType" -> "question_resource_type",
    "question_metadata.summativeAssessment" -> "question_summative_assessment",
    "question_metadata.difficultyLevel" -> "question_difficulty_level",
    "question_metadata.cognitiveDimensions" -> "question_cognitive_dimensions",
    "question_metadata.knowledgeDimensions" -> "question_knowledge_dimensions",
    "question_metadata.lexileLevel" -> "question_lexile_level",
    "question_metadata.lexileLevels" -> "question_lexile_levels",
    "question_metadata.copyrights" -> "question_copyrights",
    "question_metadata.author" -> "question_author",
    "question_authored_date_ts" -> "question_authored_date",
    "question_metadata.skillId" -> "question_skill_id",
    "question_metadata.cefrLevel" -> "question_cefr_level",
    "question_metadata.proficiency" -> "question_proficiency",
    "occurredOn"-> "occurredOn",
    "question_status" -> "question_status",
    "question_active_until" -> "question_active_until"
  )
}
