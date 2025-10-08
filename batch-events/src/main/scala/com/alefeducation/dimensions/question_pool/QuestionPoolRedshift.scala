package com.alefeducation.dimensions.question_pool

import com.alefeducation.base.SparkBatchService
import com.alefeducation.util.BatchTransformerUtility._
import com.alefeducation.util.Resources.{getSink, getSource}
import com.alefeducation.util.SparkSessionUtils
import com.alefeducation.io.data.RedshiftIO.TempTableAlias

object QuestionPoolRedshift {
  val QuestionPoolRedshiftService = "redshift-ccl-question-pool-mutated-service"
  val targetTableName = "dim_question_pool"

  private val session = SparkSessionUtils.getSession(QuestionPoolRedshiftService)
  val service = new SparkBatchService(QuestionPoolRedshiftService, session)

  val columns: Seq[String] = Seq(
    "question_pool_id",
    "question_pool_name",
    "question_pool_app_status",
    "question_pool_triggered_by",
    "question_pool_question_code_prefix",
    "question_pool_status",
    "question_pool_created_time",
    "question_pool_dw_created_time",
    "question_pool_updated_time",
    "question_pool_deleted_time"
  )

  val columnsToInsert: Map[String, String] = columns.map { column =>
    column -> s"$TempTableAlias.$column"
  }.toMap

  val columnsToUpdate: Map[String, String] = columnsToInsert -
    "question_pool_dw_created_time" +
    ("question_pool_dw_updated_time" -> s"$TempTableAlias.question_pool_dw_created_time")
  def main(args: Array[String]): Unit = {
    service.run {
      val QuestionPoolTransformed = service.readOptional(getSource(QuestionPoolRedshiftService).head, session)

      QuestionPoolTransformed.map(_.toRedshiftUpsertSink(
          sinkName = getSink(QuestionPoolRedshiftService).head,
          tableName = targetTableName,
          matchConditions = s"$targetTableName.question_pool_id = $TempTableAlias.question_pool_id",
          columnsToInsert = columnsToInsert,
          columnsToUpdate = columnsToUpdate,
          isStaging = false
        )
      )
    }
  }
}