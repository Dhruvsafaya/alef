package com.alefeducation.dimensions.question

import com.alefeducation.base.SparkBatchService
import com.alefeducation.dimensions.question.QuestionDimensionTransform.QuestionMutatedEntityPrefix
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.getSink
import com.alefeducation.util.SparkSessionUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.TimestampType

object QuestionDimensionRedshift {
  private val QuestionRedshiftService = "redshift-ccl-question-mutated-service"

  private val session = SparkSessionUtils.getSession(QuestionRedshiftService)
  val service = new SparkBatchService(QuestionRedshiftService, session)

  private def transformQuestionToRedshift(dataframe: DataFrame): DataFrame = {
    dataframe
      .withColumn("question_active_until", col("question_active_until").cast(TimestampType))
      .drop(
        "question_body", "question_lexile_levels", "question_curriculum_outcomes", "question_cognitive_dimensions",
        "question_keywords", "question_conditions_of_use", "question_copyrights", "question_validation"
      )
  }

  def main(args: Array[String]): Unit = {
    service.run {
      val QuestionTransformed = service.readOptional("transformed-ccl-question-mutated", session)
      val questionTransformedToRedshift = QuestionTransformed.map(transformQuestionToRedshift)

      questionTransformedToRedshift.map(_.toRedshiftIWHSink(
          getSink(QuestionRedshiftService).head,
          QuestionMutatedEntityPrefix,
          ids = List("question_id"),
          isStagingSink = true,
          inactiveStatus = 2
        )
      )
    }
  }
}